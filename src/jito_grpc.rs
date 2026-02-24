use std::{
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, bail, Context, Result};
use jito_protos::{
    auth::{
        auth_service_client::AuthServiceClient, GenerateAuthChallengeRequest,
        GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
    },
    bundle::Bundle,
    packet::Meta,
    searcher::{
        searcher_service_client::SearcherServiceClient, GetTipAccountsRequest, SendBundleRequest,
        SubscribeBundleResultsRequest,
    },
};
use log::{info, warn};
use jito_protos::google::protobuf::Timestamp;
use solana_keypair::{read_keypair_file, Keypair};
use solana_signer::Signer;
use solana_transaction::versioned::VersionedTransaction;
use tokio::{sync::Mutex, task::JoinHandle, time::sleep};
use tonic::{
    codegen::InterceptedService,
    service::Interceptor,
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request, Status,
};

use crate::log_utils::append_trade_log;

const AUTHORIZATION_HEADER: &str = "authorization";
const BEARER_PREFIX: &str = "Bearer ";

#[derive(Clone)]
struct AuthInterceptor {
    bearer_token: Arc<RwLock<String>>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let token = self.bearer_token.read().map_err(|_| {
            Status::internal("failed to read Jito auth token")
        })?;
        if !token.is_empty() {
            request.metadata_mut().insert(
                AUTHORIZATION_HEADER,
                format!("{BEARER_PREFIX}{token}").parse().map_err(|_| {
                    Status::internal("invalid Jito auth token metadata")
                })?,
            );
        }
        Ok(request)
    }
}

pub struct JitoGrpcClient {
    endpoint: String,
    searcher: Mutex<SearcherServiceClient<InterceptedService<Channel, AuthInterceptor>>>,
    _refresh_handle: Option<JoinHandle<()>>,
    _bundle_results_handle: Option<JoinHandle<()>>,
}

impl JitoGrpcClient {
    pub async fn from_env() -> Result<Option<Self>> {
        let endpoint = match std::env::var("JITO_GRPC_URL") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return Ok(None),
        };

        let auth_keypair = load_auth_keypair_from_env()?;
        if auth_keypair.is_none() {
            info!("[JITO] gRPC auth keypair not set; connecting without auth");
        }

        Ok(Some(Self::connect(endpoint, auth_keypair).await?))
    }

    pub async fn connect(endpoint: String, auth_keypair: Option<Arc<Keypair>>) -> Result<Self> {
        let bearer_token = Arc::new(RwLock::new(String::new()));
        let mut refresh_handle = None;

        if let Some(keypair) = auth_keypair {
            let auth_channel = create_grpc_channel(&endpoint).await?;
            let mut auth_client = AuthServiceClient::new(auth_channel);
            let (access_token, refresh_token) = auth(&mut auth_client, &keypair).await?;
            *bearer_token
                .write()
                .map_err(|_| anyhow!("failed to store Jito auth token"))? = access_token.value.clone();

            refresh_handle = Some(spawn_token_refresh_task(
                auth_client,
                bearer_token.clone(),
                refresh_token,
                access_token.expires_at_utc.clone(),
                keypair,
            ));
        }

        let searcher_channel = create_grpc_channel(&endpoint).await?;
        let interceptor = AuthInterceptor { bearer_token };
        let searcher = SearcherServiceClient::with_interceptor(searcher_channel, interceptor);

        let mut subscribe_client = searcher.clone();
        let endpoint_clone = endpoint.clone();
        let bundle_results_handle = if bundle_results_subscription_enabled() {
            Some(tokio::spawn(async move {
                sleep(Duration::from_secs(10)).await;
                loop {
                    if let Err(err) =
                        stream_bundle_results(&mut subscribe_client, &endpoint_clone).await
                    {
                        let err_text = format!("{:?}", err);
                        append_trade_log(&format!(
                            "BUNDLE_GRPC_RESULT endpoint={} result=subscribe_error err={}",
                            endpoint_clone, err_text
                        ));
                        warn!(
                            "[JITO] subscribe bundle results error: {:?} (retrying in 150s)",
                            err
                        );
                        sleep(Duration::from_secs(150)).await;
                        continue;
                    }
                    break;
                }
            }))
        } else {
            info!("[JITO] bundle results subscription disabled");
            None
        };

        Ok(Self {
            endpoint,
            searcher: Mutex::new(searcher),
            _refresh_handle: refresh_handle,
            _bundle_results_handle: bundle_results_handle,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn send_bundle(&self, txs: &[VersionedTransaction]) -> Result<String> {
        if txs.is_empty() {
            bail!("cannot submit empty bundle");
        }
        if txs.len() > 5 {
            bail!("Jito bundle exceeds maximum of 5 transactions (got {})", txs.len());
        }

        let packets = txs
            .iter()
            .map(packet_from_versioned_tx)
            .collect::<Result<Vec<_>>>()?;
        let request = SendBundleRequest {
            bundle: Some(Bundle {
                header: None,
                packets,
            }),
        };

        let mut client = self.searcher.lock().await;
        let response = client
            .send_bundle(request)
            .await
            .context("submit bundle via Jito gRPC")?;
        Ok(response.into_inner().uuid)
    }

    pub async fn get_tip_accounts(&self) -> Result<Vec<String>> {
        let mut client = self.searcher.lock().await;
        let response = client
            .get_tip_accounts(GetTipAccountsRequest {})
            .await
            .context("get tip accounts via Jito gRPC")?;
        Ok(response.into_inner().accounts)
    }
}

fn bundle_results_subscription_enabled() -> bool {
    let force = std::env::var("FORCE_JITO_GRPC_SUBSCRIBE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    if force {
        return true;
    }
    let disabled = std::env::var("DISABLE_JITO_GRPC_SUBSCRIBE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    if disabled {
        return false;
    }
    let dry_run = std::env::var("ARBITRAGE_DRY_RUN_ONLY")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);
    !dry_run
}

async fn stream_bundle_results(
    client: &mut SearcherServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    endpoint: &str,
) -> Result<()> {
    let response = client
        .subscribe_bundle_results(SubscribeBundleResultsRequest {})
        .await
        .context("subscribe bundle results via Jito gRPC")?;
    let mut stream = response.into_inner();

    loop {
        match stream.message().await {
            Ok(Some(result)) => {
                let summary = format_bundle_result(&result);
                append_trade_log(&format!(
                    "BUNDLE_GRPC_RESULT endpoint={} bundle_id={} {}",
                    endpoint, result.bundle_id, summary
                ));
            }
            Ok(None) => {
                append_trade_log(&format!(
                    "BUNDLE_GRPC_RESULT endpoint={} result=stream_end",
                    endpoint
                ));
                break;
            }
            Err(err) => {
                append_trade_log(&format!(
                    "BUNDLE_GRPC_RESULT endpoint={} result=stream_error err={:?}",
                    endpoint, err
                ));
                return Err(err.into());
            }
        }
    }

    Ok(())
}

fn format_bundle_result(result: &jito_protos::bundle::BundleResult) -> String {
    use jito_protos::bundle;

    let Some(kind) = result.result.as_ref() else {
        return "result=missing".to_string();
    };

    match kind {
        bundle::bundle_result::Result::Accepted(accepted) => format!(
            "result=accepted slot={} validator={}",
            accepted.slot, accepted.validator_identity
        ),
        bundle::bundle_result::Result::Processed(processed) => format!(
            "result=processed slot={} validator={} bundle_index={}",
            processed.slot, processed.validator_identity, processed.bundle_index
        ),
        bundle::bundle_result::Result::Finalized(_) => "result=finalized".to_string(),
        bundle::bundle_result::Result::Dropped(dropped) => {
            let reason = bundle::DroppedReason::try_from(dropped.reason)
                .ok()
                .map(|value| value.as_str_name())
                .unwrap_or("Unknown");
            format!("result=dropped reason={}", reason)
        }
        bundle::bundle_result::Result::Rejected(rejected) => {
            let Some(reason) = rejected.reason.as_ref() else {
                return "result=rejected reason=unknown".to_string();
            };
            match reason {
                bundle::rejected::Reason::StateAuctionBidRejected(info) => {
                    let mut out = format!(
                        "result=rejected reason=state_auction_bid_rejected auction_id={} simulated_bid_lamports={}",
                        info.auction_id, info.simulated_bid_lamports
                    );
                    if let Some(msg) = &info.msg {
                        if !msg.is_empty() {
                            out.push_str(&format!(" msg={}", msg));
                        }
                    }
                    out
                }
                bundle::rejected::Reason::WinningBatchBidRejected(info) => {
                    let mut out = format!(
                        "result=rejected reason=winning_batch_bid_rejected auction_id={} simulated_bid_lamports={}",
                        info.auction_id, info.simulated_bid_lamports
                    );
                    if let Some(msg) = &info.msg {
                        if !msg.is_empty() {
                            out.push_str(&format!(" msg={}", msg));
                        }
                    }
                    out
                }
                bundle::rejected::Reason::SimulationFailure(info) => {
                    let mut out = format!(
                        "result=rejected reason=simulation_failure tx_signature={}",
                        info.tx_signature
                    );
                    if let Some(msg) = &info.msg {
                        if !msg.is_empty() {
                            out.push_str(&format!(" msg={}", msg));
                        }
                    }
                    out
                }
                bundle::rejected::Reason::InternalError(info) => {
                    let mut out = "result=rejected reason=internal_error".to_string();
                    if !info.msg.is_empty() {
                        out.push_str(&format!(" msg={}", info.msg));
                    }
                    out
                }
                bundle::rejected::Reason::DroppedBundle(info) => {
                    let mut out = "result=rejected reason=dropped_bundle".to_string();
                    if !info.msg.is_empty() {
                        out.push_str(&format!(" msg={}", info.msg));
                    }
                    out
                }
            }
        }
    }
}

fn packet_from_versioned_tx(tx: &VersionedTransaction) -> Result<jito_protos::packet::Packet> {
    let data = bincode::serialize(tx).context("serialize transaction for Jito bundle")?;
    Ok(jito_protos::packet::Packet {
        data: data.clone(),
        meta: Some(Meta {
            size: data.len() as u64,
            addr: String::new(),
            port: 0,
            flags: None,
            sender_stake: 0,
        }),
    })
}

fn load_auth_keypair_from_env() -> Result<Option<Arc<Keypair>>> {
    let path = std::env::var("JITO_GRPC_AUTH_KEYPAIR")
        .or_else(|_| std::env::var("JITO_AUTH_KEYPAIR"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let Some(path) = path else {
        return Ok(None);
    };

    let keypair = read_keypair_file(&path)
        .map_err(|err| anyhow!("load Jito auth keypair {path}: {err}"))?;
    Ok(Some(Arc::new(keypair)))
}

async fn create_grpc_channel(url: &str) -> Result<Channel> {
    let mut endpoint = Endpoint::from_shared(url.to_string())
        .map_err(|err| anyhow!("invalid Jito gRPC URL {url}: {err}"))?;
    if url.starts_with("https") {
        let tls = ClientTlsConfig::new().with_native_roots();
        endpoint = endpoint.tls_config(tls)?;
    }
    Ok(endpoint.connect().await?)
}

async fn auth(
    auth_service_client: &mut AuthServiceClient<Channel>,
    keypair: &Keypair,
) -> Result<(Token, Token)> {
    let challenge_resp = auth_service_client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: keypair.pubkey().as_ref().to_vec(),
        })
        .await?
        .into_inner();
    let challenge = format!("{}-{}", keypair.pubkey(), challenge_resp.challenge);
    let signed_challenge = keypair.sign_message(challenge.as_bytes()).as_ref().to_vec();

    let tokens = auth_service_client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: keypair.pubkey().as_ref().to_vec(),
            signed_challenge,
        })
        .await?
        .into_inner();

    let access_token = tokens
        .access_token
        .ok_or_else(|| anyhow!("missing access token from Jito auth"))?;
    let refresh_token = tokens
        .refresh_token
        .ok_or_else(|| anyhow!("missing refresh token from Jito auth"))?;
    Ok((access_token, refresh_token))
}

fn spawn_token_refresh_task(
    mut auth_service_client: AuthServiceClient<Channel>,
    bearer_token: Arc<RwLock<String>>,
    refresh_token: Token,
    access_token_expiration: Option<Timestamp>,
    keypair: Arc<Keypair>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut refresh_token = refresh_token;
        let mut access_token_expiration = access_token_expiration;

        loop {
            let access_ttl = token_ttl(access_token_expiration.as_ref());
            let refresh_ttl = token_ttl(refresh_token.expires_at_utc.as_ref());

            let access_expires_soon = access_ttl < Duration::from_secs(5 * 60);
            let refresh_expires_soon = refresh_ttl < Duration::from_secs(5 * 60);

            if refresh_expires_soon {
                match auth(&mut auth_service_client, &keypair).await {
                    Ok((new_access, new_refresh)) => {
                        if let Ok(mut guard) = bearer_token.write() {
                            *guard = new_access.value.clone();
                        }
                        access_token_expiration = new_access.expires_at_utc.clone();
                        refresh_token = new_refresh;
                        info!("[JITO] refreshed gRPC auth tokens");
                    }
                    Err(err) => {
                        warn!("[JITO] failed to refresh gRPC auth tokens: {err}");
                    }
                }
            } else if access_expires_soon {
                match auth_service_client
                    .refresh_access_token(RefreshAccessTokenRequest {
                        refresh_token: refresh_token.value.clone(),
                    })
                    .await
                {
                    Ok(resp) => {
                        if let Some(access_token) = resp.into_inner().access_token {
                            if let Ok(mut guard) = bearer_token.write() {
                                *guard = access_token.value.clone();
                            }
                            access_token_expiration = access_token.expires_at_utc.clone();
                            info!("[JITO] refreshed gRPC access token");
                        }
                    }
                    Err(err) => {
                        warn!("[JITO] failed to refresh gRPC access token: {err}");
                    }
                }
            } else {
                sleep(Duration::from_secs(60)).await;
            }
        }
    })
}

fn token_ttl(ts: Option<&Timestamp>) -> Duration {
    let Some(ts) = ts else {
        return Duration::from_secs(0);
    };
    let Some(expire_at) = timestamp_to_system_time(ts) else {
        return Duration::from_secs(0);
    };
    expire_at
        .duration_since(SystemTime::now())
        .unwrap_or_else(|_| Duration::from_secs(0))
}

fn timestamp_to_system_time(ts: &Timestamp) -> Option<SystemTime> {
    if ts.seconds < 0 || ts.nanos < 0 {
        return None;
    }
    let seconds = ts.seconds as u64;
    let nanos = ts.nanos as u64;
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(seconds) + Duration::from_nanos(nanos))
}

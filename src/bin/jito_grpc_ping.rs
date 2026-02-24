use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let Some(client) = arb::jito_grpc::JitoGrpcClient::from_env().await? else {
        println!("JITO_GRPC_URL is not set; nothing to test.");
        return Ok(());
    };

    match client.get_tip_accounts().await {
        Ok(accounts) => {
            println!("gRPC OK: got {} tip accounts", accounts.len());
        }
        Err(err) => {
            let mut exhausted = false;
            for cause in err.chain() {
                if let Some(status) = cause.downcast_ref::<tonic::Status>() {
                    if status.code() == tonic::Code::ResourceExhausted {
                        exhausted = true;
                        break;
                    }
                }
            }
            if exhausted {
                println!("gRPC OK but rate limited (ResourceExhausted).");
            } else {
                return Err(err);
            }
        }
    }

    Ok(())
}

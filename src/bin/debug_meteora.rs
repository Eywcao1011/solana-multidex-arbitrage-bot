use reqwest;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "https://dlmm-api.meteora.ag/pair/all";
    println!("Fetching from {}", url);
    
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;
        
    let resp = client.get(url).send().await?;
    let text = resp.text().await?;
    
    println!("Response length: {} bytes", text.len());
    println!("Response preview: {}", text); // 打印整个内容，因为只有 3KB
    
    let v: Value = serde_json::from_str(&text)?;
    
    if let Value::Array(arr) = v {
        println!("JSON is an array with {} items", arr.len());
        
        // Search for the token manually
        let target_mint = "8J69rbLTzWWgUJziFY8jeu5tDwEPBwUz4pKBMr5rpump";
        let mut found = false;
        
        for item in arr {
            let mint_x = item["mint_x"].as_str().unwrap_or("");
            let mint_y = item["mint_y"].as_str().unwrap_or("");
            
            if mint_x == target_mint || mint_y == target_mint {
                println!("Found Token in raw JSON!");
                let pair_address = item["pair_address"].as_str().unwrap_or("?");
                let name = item["name"].as_str().unwrap_or("?");
                let mint_x_addr = mint_x;
                let mint_y_addr = mint_y;
                println!("  Address: {}", pair_address);
                println!("  Name: {}", name);
                println!("  Mint X: {}", mint_x_addr);
                println!("  Mint Y: {}", mint_y_addr);
                
                // Check liquidity
                let liq_str = item["liquidity"].as_str().unwrap_or("0");
                let liq: f64 = liq_str.parse().unwrap_or(0.0);
                println!("  Liquidity string: {}", liq_str);
                println!("  Parsed TVL: ${:.2}", liq);
                
                if liq < 20_000.0 {
                    println!("  ❌ Would be DROPPED by $20,000 pre-filter");
                } else {
                    println!("  ✅ Would pass $20,000 pre-filter");
                }
                found = true;
            }
        }
        
        if !found {
            println!("❌ Token NOT found in Meteora API response!");
        }
        
    } else {
        println!("JSON is NOT an array!");
    }
    
    Ok(())
}

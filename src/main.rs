use cassandra_protocol::events::ServerEvent;
use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use rand::Rng;
use std::sync::Arc;
use tokio::time::{timeout, Duration};

#[tokio::main]
async fn main() {
    let mut rng = rand::thread_rng();

    let user = "cassandra";
    let password = "cassandra";
    let auth = StaticPasswordAuthenticatorProvider::new(&user, &password);
    let config = NodeTcpConfigBuilder::new()
        .with_contact_point("172.16.1.2:9042".into())
        .with_authenticator_provider(Arc::new(auth))
        .build()
        .await
        .unwrap();

    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), config).build();
    let mut event_recv = session.create_event_receiver();

    let mut tries = 0;
    loop {
        if tries > 3 {
            panic!("did not receive the event after 3 tries.");
        }

        let n: u8 = rng.gen();
        let create_ks = format!("CREATE KEYSPACE IF NOT EXISTS test_ks_{} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }};", n);

        session
            .query(create_ks)
            .await
            .expect("Keyspace creation error");

        match timeout(Duration::from_secs(10), event_recv.recv()).await {
            Ok(recvd) => {
                if let Ok(event) = recvd {
                    println!("recevied event {:?}", event);
                    assert!(matches!(event, ServerEvent::SchemaChange { .. }));
                    break;
                };
            }
            Err(err) => {
                println!("timed out waiting for event");
                tries += 1;
            }
        }
    }
}

use super::*;

pub struct Rack;

impl Rack {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("racks")
            .find(doc! {}, find_opts(doc! {"displayName": 1}, doc! {"_id": 1}))
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            postgres
                .execute(
                    "INSERT INTO stock_location (name) VALUES ($1) on conflict do nothing",
                    &[&d.get_str("displayName").unwrap()],
                )
                .await
                .unwrap();
        }
    }
}

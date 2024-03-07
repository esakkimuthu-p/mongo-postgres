use super::*;

pub struct SaleIncharge;

impl SaleIncharge {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("sale_incharges")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            postgres
                .execute(
                    "INSERT INTO sale_incharges (name,code) VALUES ($1,$2)",
                    &[&d.get_str("name").unwrap(), &d.get_str("code").unwrap()],
                )
                .await
                .unwrap();
        }
    }
}

use super::*;

pub struct Doctor;

impl Doctor {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("doctors")
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
                    "INSERT INTO doctor (name,license_no) VALUES ($1, $2)",
                    &[&d.get_str("name").unwrap(), &d.get_str("licenseNo").ok()],
                )
                .await
                .unwrap();
        }
    }
}

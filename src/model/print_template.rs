use super::*;

pub struct PrintTemplate;

impl PrintTemplate {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("print_templates")
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
            let object_id = d.get_object_id("_id").unwrap();
            postgres
                .execute(
                    "INSERT INTO print_template (name,template,layout,voucher_mode) 
                    VALUES 
                    ($1, $2, $3, $4)",
                    &[
                        &d.get_str("name").unwrap(),
                        &d.get_str("template").unwrap(),
                        &d.get_str("layout").unwrap(),
                        &d.get_str("voucherMode").ok().map(|x| &x[0..2]),
                    ],
                )
                .await
                .unwrap();
        }
    }
}

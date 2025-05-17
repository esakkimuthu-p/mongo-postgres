use mongodb::bson::oid::ObjectId;

use super::*;

pub struct VoucherNumSequence;

impl VoucherNumSequence {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let voucher_types = mongodb
            .collection::<Document>("voucher_types")
            .find(
                doc! {"postgres": {"$exists": true}},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let financial_years = mongodb
            .collection::<Document>("financial_years")
            .find(
                doc! {"postgres": {"$exists": true}},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let financial_year_ids = financial_years
            .iter()
            .map(|x| x.get_object_id("_id").unwrap())
            .collect::<Vec<ObjectId>>();
        let mut cur = mongodb
            .collection::<Document>("voucher_numberings")
            .find(doc! {"fYear": {"$in":financial_year_ids }}, None)
            .await
            .unwrap();

        while let Some(Ok(d)) = cur.next().await {
            let voucher_type = d.get_object_id("voucherTypeId").unwrap();
            let voucher_type = voucher_types.iter().find_map(|x| {
                (x.get_object_id("_id").unwrap() == voucher_type)
                    .then_some(x._get_i32("postgres").unwrap())
            });
            if let Some(v_type) = voucher_type {
                let branch = branches
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                            .then_some(x._get_i32("postgres").unwrap())
                    })
                    .unwrap();
                let f_year = financial_years
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == d.get_object_id("fYear").unwrap())
                            .then_some(x._get_i32("postgres").unwrap())
                    })
                    .unwrap();
                postgres
                .execute(
                    "INSERT INTO voucher_numbering (branch_id,voucher_type_id,f_year_id,seq) VALUES ($1, $2,$3,$4)",
                    &[&branch,&v_type, &f_year,&d._get_i32("sequence").unwrap()],
                )
                .await
                .unwrap();
            }
        }
    }
}

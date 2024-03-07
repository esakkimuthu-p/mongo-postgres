use super::*;

pub const DEFAULT_NAMES: [&str; 15] = [
    "CASH",
    "SALE",
    "PURCHASE",
    "CGST_PAYABLE",
    "SGST_PAYABLE",
    "IGST_PAYABLE",
    "CESS_PAYABLE",
    "CGST_RECEIVABLE",
    "SGST_RECEIVABLE",
    "IGST_RECEIVABLE",
    "CESS_RECEIVABLE",
    "ROUNDED_OFF",
    "DISCOUNT_GIVEN",
    "DISCOUNT_RECEIVED",
    "INVENTORY_ASSET",
];
pub struct Account;

impl Account {
    pub async fn map(mongodb: &Database) {
        let updates = vec![
            doc! {"q": { "defaultName": "CASH"}, "u": {"$set": {"postgres": 1} }},
            doc! {"q": { "defaultName" : {"$regex": "SALE"}}, "u": {"$set": {"postgres": 2}}, "multi": true},
            doc! {"q": { "defaultName" : {"$regex":"PURCHASE"}}, "u": {"$set": {"postgres": 3} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"CGST_PAYABLE"}}, "u": {"$set": {"postgres": 4} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"SGST_PAYABLE"}}, "u": {"$set": {"postgres": 5} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"IGST_PAYABLE"}}, "u": {"$set": {"postgres": 6} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"CESS_PAYABLE"}}, "u": {"$set": {"postgres": 7} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"CGST_RECEIVABLE"}}, "u": {"$set": {"postgres": 8} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"SGST_RECEIVABLE"}}, "u": {"$set": {"postgres": 9} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"IGST_RECEIVABLE"}}, "u": {"$set": {"postgres": 10} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"CESS_RECEIVABLE"}}, "u": {"$set": {"postgres": 11} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"ROUNDED_OFF"}}, "u": {"$set": {"postgres": 12} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"DISCOUNT_GIVEN"}}, "u": {"$set": {"postgres": 13} },"multi": true},
            doc! {"q": { "defaultName" : {"$regex":"DISCOUNT_RECEIVED"}}, "u": {"$set": {"postgres": 14} },"multi": true},
            doc! {"q": { "defaultName": "INVENTORY_ASSET"}, "u": {"$set": {"postgres": 16} }},
        ];
        let command = doc! {
            "update": "accounts",
            "updates": &updates
        };
        mongodb.run_command(command, None).await.unwrap();
        mongodb
            .collection::<Document>("accounts")
            .update_many(
                doc! {},
                vec![doc! {"$set": {"postgresAccountType": "$accountType"}}],
                None,
            )
            .await
            .unwrap();
        mongodb
            .collection::<Document>("accounts")
            .update_many(
                doc! {"accountType": {"$in": ["ACCOUNT_PAYABLE", "TRADE_PAYABLE"]}},
                doc! {"$set": {"postgresAccountType": "SUNDRY_CREDITOR"}},
                None,
            )
            .await
            .unwrap();
        mongodb
            .collection::<Document>("accounts")
            .update_many(
                doc! {"accountType": {"$in": ["ACCOUNT_RECEIVABLE", "TRADE_RECEIVABLE"]}},
                doc! {"$set": {"postgresAccountType": "SUNDRY_DEBTOR"}},
                None,
            )
            .await
            .unwrap();
        mongodb
            .collection::<Document>("accounts")
            .update_many(
                doc! {"accountType": {"$in": ["GST_PAYABLE", "GST_RECEIVABLE"]}},
                doc! {"$set": {"postgresAccountType": "DUTIES_AND_TAXES"}},
                None,
            )
            .await
            .unwrap();
    }

    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let regex = DEFAULT_NAMES.join("|");

        let mut cur = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {"defaultName":{"$not":{"$regex":regex}}},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 100;
        let mut updates = Vec::new();
        let mut parent_ref_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let bill_wise_detail = ["SUNDRY_CREDITOR", "SUNDRY_DEBTOR"]
                .contains(&d.get_str("postgresAccountType").unwrap());
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO accounts 
                    (
                        id,name,alias_name,account_type,gst_tax,sac_code,bill_wise_detail
                    )
                    OVERRIDING SYSTEM VALUE
                    VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("aliasName").ok(),
                        &d.get_str("postgresAccountType").unwrap(),
                        &d.get_str("tax").ok(),
                        &d.get_str("sacCode").ok(),
                        &bill_wise_detail,
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            parent_ref_updates.push(doc! {
                "q": { "parentAccount": object_id },
                "u": { "$set": { "postgresParent": id} },
                "multi": true
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "accounts",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "accounts",
                "updates": &parent_ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        let mut cur = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {"postgresParent":{"$exists":true}},
                find_opts(
                    doc! {"_id": 0, "postgres": 1, "postgresParent": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            postgres
                .execute(
                    "UPDATE accounts SET parent = $2 WHERE id = $1",
                    &[
                        &d.get_i32("postgres").unwrap(),
                        &d.get_i32("postgresParent").unwrap(),
                    ],
                )
                .await
                .unwrap();
        }
    }
}

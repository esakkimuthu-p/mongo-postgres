use mongodb::bson::oid::ObjectId;

use super::*;

pub const DEFAULT_NAMES: [&str; 11] = [
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
        ];
        let command = doc! {
            "update": "accounts",
            "updates": &updates
        };
        mongodb.run_command(command, None).await.unwrap();

        for coll in VOUCHER_COLLECTION {
            let mut id = 0;
            println!("collection {} started", coll);
            for def_name in DEFAULT_NAMES {
                println!("default_name {} started", def_name);
                let acc_ids = mongodb
                    .collection::<Document>("accounts")
                    .find(doc! {"defaultName":{"$regex":def_name}}, None)
                    .await
                    .unwrap()
                    .try_collect::<Vec<Document>>()
                    .await
                    .unwrap()
                    .iter()
                    .map(|x| x.get_object_id("_id").unwrap())
                    .collect::<Vec<ObjectId>>();
                let options = UpdateOptions::builder()
                    .array_filters(vec![doc! { "elm.account": {"$in":acc_ids.clone()} }])
                    .build();
                id += 1;
                mongodb
                    .collection::<Document>(coll)
                    .update_many(
                        doc! {"acTrns.account": {"$in": acc_ids}},
                        doc! {"$set": { "acTrns.$[elm].postgresAccount": id} },
                        options,
                    )
                    .await
                    .unwrap();
                println!("default_name {} ended", def_name);
            }
            println!("collection {} end", coll);
        }
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
        let mut id: i32 = 11;
        let mut updates = Vec::new();
        let mut ref_updates = Vec::new();
        let mut voucher_ref_updates = Vec::new();
        let mut parent_ref_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO accounts 
                    (
                        id,name,display_name,val_name,alias_name,val_alias_name,account_type
                    ) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("displayName").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                        &d.get_str("aliasName").ok(),
                        &d.get_str("aliasName").ok().map(val_name),
                        &d.get_str("accountType").unwrap(),
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
            ref_updates.push(doc! {
                "q": { "account": object_id },
                "u": { "$set":{"postgresAccount": id }},
                "multi": true,
            });
            voucher_ref_updates.push(doc! {
                "q": { "acTrns": {"$elemMatch": {"account": object_id }} },
                "u": { "$set": { "acTrns.$[elm].postgresAccount": id} },
                "multi": true,
                "arrayFilters": [ { "elm.account": {"$eq":object_id} } ]
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

            let command = doc! {
                "update": "account_openings",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "branches",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            for coll in VOUCHER_COLLECTION {
                println!("collection {} start", coll);
                let command = doc! {
                    "update": coll,
                    "updates": &voucher_ref_updates
                };
                mongodb.run_command(command, None).await.unwrap();
                println!("collection {} end", coll);
            }
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

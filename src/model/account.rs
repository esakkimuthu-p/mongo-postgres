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
            doc! {"q": { "accountType": "CURRENT_ASSET"}, "u": {"$set": {"postgresAccountType": 1} }, "multi": true},
            doc! {"q": { "accountType": "CURRENT_LIABILITY"}, "u": {"$set": {"postgresAccountType": 2} }, "multi": true},
            doc! {"q": { "accountType": "DIRECT_INCOME"}, "u": {"$set": {"postgresAccountType": 3} }, "multi": true},
            doc! {"q": { "accountType": "INDIRECT_INCOME"}, "u": {"$set": {"postgresAccountType": 4} }, "multi": true},
            doc! {"q": { "accountType": "SALE"}, "u": {"$set": {"postgresAccountType": 5} }, "multi": true},
            doc! {"q": { "accountType": "DIRECT_EXPENSE"}, "u": {"$set": {"postgresAccountType": 6} }, "multi": true},
            doc! {"q": { "accountType": "INDIRECT_EXPENSE"}, "u": {"$set": {"postgresAccountType": 7} }, "multi": true},
            doc! {"q": { "accountType": "PURCHASE"}, "u": {"$set": {"postgresAccountType": 8} }, "multi": true},
            doc! {"q": { "accountType": "FIXED_ASSET"}, "u": {"$set": {"postgresAccountType": 9} }, "multi": true},
            doc! {"q": { "accountType": "LONGTERM_LIABILITY"}, "u": {"$set": {"postgresAccountType": 10} }, "multi": true},
            doc! {"q": { "accountType": "EQUITY"}, "u": {"$set": {"postgresAccountType": 11} }, "multi": true},
            doc! {"q": { "accountType": "STOCK"}, "u": {"$set": {"postgresAccountType": 12} }, "multi": true},
            doc! {"q": { "accountType": "BANK_ACCOUNT"}, "u": {"$set": {"postgresAccountType": 13} }, "multi": true},
            doc! {"q": { "accountType": "EFT_ACCOUNT"}, "u": {"$set": {"postgresAccountType": 14} }, "multi": true},
            doc! {"q": { "accountType": "TDS_RECEIVABLE"}, "u": {"$set": {"postgresAccountType": 15} }, "multi": true},
            doc! {"q": { "accountType": {"$in": ["ACCOUNT_RECEIVABLE", "TRADE_RECEIVABLE"]}}, "u": {"$set": {"postgresAccountType": 16} }, "multi": true},
            doc! {"q": { "accountType": "CASH"}, "u": {"$set": {"postgresAccountType": 17} }, "multi": true},
            doc! {"q": { "accountType": "BANK_OD_ACCOUNT"}, "u": {"$set": {"postgresAccountType": 18} }, "multi": true},
            doc! {"q": { "accountType": {"$in": ["ACCOUNT_PAYABLE", "TRADE_PAYABLE"]}}, "u": {"$set": {"postgresAccountType": 19} }, "multi": true},
            doc! {"q": { "accountType": "BRANCH_TRANSFER"}, "u": {"$set": {"postgresAccountType": 20} }, "multi": true},
            doc! {"q": { "accountType": "TDS_PAYABLE"}, "u": {"$set": {"postgresAccountType": 21} }, "multi": true},
            doc! {"q": { "accountType": {"$in": ["GST_PAYABLE", "GST_RECEIVABLE"]}}, "u": {"$set": {"postgresAccountType": 22} }, "multi": true},
        ];
        let command = doc! {
            "update": "accounts",
            "updates": &updates
        };
        mongodb.run_command(command, None).await.unwrap();
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
        let mut updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let mut bill_wise_detail = false;
            if d._get_i32("postgresAccountType").unwrap() == 16 {
                bill_wise_detail = true;
            } else if d._get_i32("postgresAccountType").unwrap() == 19 {
                bill_wise_detail = true;
            }
            let object_id = d.get_object_id("_id").unwrap();

            let id: i32 = postgres
                .query_one(
                    "INSERT INTO account 
                    (
                        name,alias_name,account_type_id,gst_tax_id,sac_code,
                        bill_wise_detail,contact_type, transaction_enabled
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, 'ACCOUNT', true) returning id",
                    &[
                        &d.get_str("name").unwrap(),
                        &d.get_str("aliasName").ok(),
                        &d._get_i32("postgresAccountType").unwrap(),
                        &d.get_str("tax").ok(),
                        &d.get_str("sacCode").ok(),
                        &bill_wise_detail,
                    ],
                )
                .await
                .unwrap()
                .get(0);
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "accounts",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}

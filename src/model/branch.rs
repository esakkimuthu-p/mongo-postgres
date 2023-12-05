use super::*;

pub struct Branch;

impl Branch {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 0;
        let mut updates = Vec::new();
        let mut ref_updates = Vec::new();
        let mut ref_updates2 = Vec::new();
        let mut inv_branch_updates = Vec::new();
        let mut ref_branch_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            let contact_info = d
                .get_document("contactInfo")
                .ok()
                .map(|x| serde_json::to_value(x).unwrap());
            let address_info = d.get_document("addressInfo").ok().map(|x| {
                let mut y = x.clone();
                if let Ok(a) = x.get_str("alternateMobile") {
                    y.remove("alternateMobile");
                    y.insert("alternate_mobile", a);
                }
                if let Ok(a) = x.get_str("contactPerson") {
                    y.remove("contactPerson");
                    y.insert("contact_person", a);
                }
                serde_json::to_value(y).unwrap()
            });
            let misc = d
                .get_str("licenseNo")
                .ok()
                .map(|x| serde_json::json!({"license_no": x}));
            let members = d
                .get_array("postgresMembers")
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.as_i32().unwrap())
                .collect::<Vec<i32>>();
            postgres
                .execute(
                    "INSERT INTO branches 
                        (id,name,val_name,display_name,contact_info,address_info, gst_registration, voucher_no_prefix, misc, members, account) 
                    OVERRIDING SYSTEM VALUE VALUES 
                        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                        &d.get_str("displayName").unwrap(),
                        &contact_info,
                        &address_info,
                        &d.get_i32("postgresGst").unwrap(),
                        &d.get_str("voucherNoPrefix").unwrap(),
                        &misc,
                        &members,
                        &d.get_i32("postgresAccount").unwrap(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            ref_updates.push(doc! {
                "q": { "branch":object_id },
                "u": { "$set": { "postgresBranch": id} },
                "multi": true,
            });
            ref_updates2.push(doc! {
                "q": { "altBranch":object_id },
                "u": { "$set": { "postgresAltBranch": id} },
                "multi": true,
            });
            inv_branch_updates.push(doc! {
                "q": { "branchDetails.branch": object_id  },
                "u": { "$set": { "branchDetails.$[elm].postgresBranch": id} },
                "multi": true,
                "arrayFilters": [ { "elm.branch": {"$eq":object_id} } ]
            });
            ref_branch_updates.push(doc! {
                "q": { "branches": object_id },
                "u": { "$addToSet": { "postgresBranches": id} },
                "multi": true
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "branches",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "batches",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "desktop_clients",
                "updates": &ref_branch_updates
            };
            mongodb.run_command(command, None).await.unwrap();
            for coll in VOUCHER_COLLECTION {
                let command = doc! {
                    "update": coll,
                    "updates": &ref_updates
                };
                mongodb.run_command(command, None).await.unwrap();
            }
            let command = doc! {
                "update": "stock_transfers",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "stock_transfers",
                "updates": &ref_updates2
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "stock_adjustments",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "material_conversions",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "manufacturing_journals",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "account_openings",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "inventory_openings",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "inventories",
                "updates": &inv_branch_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}

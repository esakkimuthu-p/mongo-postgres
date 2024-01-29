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
        let mut inv_branch_updates = Vec::new();
        let mut ref_branch_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            let mut mobile = None;
            let mut alternate_mobile = None;
            let mut email = None;
            let mut telephone = None;
            let mut contact_person = None;
            if let Ok(contact_info) = d.get_document("contactInfo") {
                mobile = contact_info.get_str("mobile").ok();
                alternate_mobile = contact_info.get_str("alternateMobile").ok();
                email = contact_info.get_str("email").ok();
                telephone = contact_info.get_str("telephone").ok();
                contact_person = contact_info.get_str("contactPerson").ok();
            }
            let mut address = None;
            let mut city = None;
            let mut pincode = None;
            let mut state = None;
            let mut country = None;
            if let Ok(address_info) = d.get_document("addressInfo") {
                address = address_info.get_str("address").ok();
                city = address_info.get_str("city").ok();
                pincode = address_info.get_str("pincode").ok();
                state = Some("33");
                country = Some("INDIA");
            }
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
                        (id,name,mobile,alternate_mobile,email,telephone,contact_person,address,
                            city,pincode,state,country, gst_registration, voucher_no_prefix, 
                            misc, members, account) 
                    OVERRIDING SYSTEM VALUE VALUES 
                        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &mobile,
                        &alternate_mobile,
                        &email,
                        &telephone,
                        &contact_person,
                        &address,
                        &city,
                        &pincode,
                        &state,
                        &country,
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
            // ref_updates2.push(doc! {
            //     "q": { "altBranch":object_id },
            //     "u": { "$set": { "postgresAltBranch": id} },
            //     "multi": true,
            // });
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
            // let command = doc! {
            //     "update": "stock_transfers",
            //     "updates": &ref_updates
            // };
            // mongodb.run_command(command, None).await.unwrap();

            // let command = doc! {
            //     "update": "stock_transfers",
            //     "updates": &ref_updates2
            // };
            // mongodb.run_command(command, None).await.unwrap();

            // let command = doc! {
            //     "update": "stock_adjustments",
            //     "updates": &ref_updates
            // };
            // mongodb.run_command(command, None).await.unwrap();

            // let command = doc! {
            //     "update": "material_conversions",
            //     "updates": &ref_updates
            // };
            // mongodb.run_command(command, None).await.unwrap();

            // let command = doc! {
            //     "update": "manufacturing_journals",
            //     "updates": &ref_updates
            // };
            // mongodb.run_command(command, None).await.unwrap();

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

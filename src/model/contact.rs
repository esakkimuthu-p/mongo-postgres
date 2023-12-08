use super::*;

pub struct Contact;

impl Contact {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("contacts")
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
        let mut cus_ref_updates = Vec::new();
        let mut ven_ref_updates = Vec::new();
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

            let mut gst_info = doc! {};
            if let Ok(gst) = d.get_document("gstInfo") {
                gst_info.insert("reg_type", gst.get_str("regType").unwrap());
                if let Ok(loc) = gst.get_str("location") {
                    gst_info.insert("location", loc);
                }
                if let Ok(no) = gst.get_str("gstNo") {
                    gst_info.insert("gst_no", no);
                }
            }
            let gst_info =
                (!gst_info.is_empty()).then_some(serde_json::to_value(gst_info).unwrap());

            postgres
                .execute(
                    "ALTER TABLE contacts ALTER COLUMN contact_type TYPE TEXT",
                    &[],
                )
                .await
                .unwrap();
            postgres
                .execute(
                    "INSERT INTO contacts 
                        (id,name,val_name,display_name,contact_type,short_name,pan_no,aadhar_no,gst_info,
                            contact_info,address_info,tracking_account, credit_account)
                    OVERRIDING SYSTEM VALUE VALUES 
                        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                        &d.get_str("displayName").unwrap(),
                        &d.get_str("contactType").unwrap().to_owned(),
                        &d.get_str("shortName").ok(),
                        &d.get_str("panNo").ok(),
                        &d.get_str("aadharNo").ok(),
                        &gst_info,
                        &contact_info,
                        &address_info,
                        &d.get_i32("postgresCrAcc").ok().is_some(),
                        &d.get_i32("postgresCrAcc").ok(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            cus_ref_updates.push(doc! {
                "q": { "customer":object_id },
                "u": { "$set": { "postgresContact": id} },
                "multi": true,
            });
            ven_ref_updates.push(doc! {
                "q": { "vendor":object_id },
                "u": { "$set": { "postgresContact": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "contacts",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "patients",
                "updates": &cus_ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
            for coll in ["sales", "credit_notes"] {
                let command = doc! {
                    "update": coll,
                    "updates": &cus_ref_updates
                };
                mongodb.run_command(command, None).await.unwrap();
            }
            for coll in ["purchases", "debit_notes"] {
                let command = doc! {
                    "update": coll,
                    "updates": &cus_ref_updates
                };
                mongodb.run_command(command, None).await.unwrap();
            }
            let command = doc! {
                "update": "inventories",
                "updates": &ven_ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        postgres
            .execute(
                "ALTER table contacts ALTER COLUMN contact_type TYPE typ_contact_type using contact_type::typ_contact_type",
                &[],
            )
            .await
            .unwrap();
    }
}

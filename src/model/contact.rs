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
                    doc! {"contactType": 1, "_id": 1},
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
            let mut gst_reg_type = "UNREGISTERED";
            let mut gst_loc = Some("33");
            let mut gst_no = None;
            if let Ok(gst) = d.get_document("gstInfo") {
                match gst.get_str("regType").unwrap() {
                    "CONSUMER" => gst_reg_type = "UNREGISTERED",
                    "OVERSEAS" | "DEEMED_EXPORT" => gst_reg_type = "IMPORT_EXPORT",
                    _ => gst_reg_type = gst.get_str("regType").unwrap(),
                }
                gst_loc = gst.get_str("location").ok();
                gst_no = gst.get_str("gstNo").ok();
            }

            let mut table_name = "customers";
            if ["VENDOR", "PAYABLE", "EMLOYEE"].contains(&d.get_str("contactType").unwrap()) {
                table_name = "vendors";
            }
            postgres
                .execute(
                    &format!(
                        "INSERT INTO {} 
                        (id,name,short_name,pan_no,aadhar_no,gst_reg_type,gst_location,gst_no,
                            mobile,alternate_mobile,email,telephone,contact_person,address,
                            city,pincode,state,country,credit_account, tracking_account) 
                    OVERRIDING SYSTEM VALUE VALUES 
                        ($1,$2,$3,$4,$5,$6::TEXT::typ_gst_reg_type,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,20)",
                        table_name
                    ),
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("shortName").ok(),
                        &d.get_str("panNo").ok(),
                        &d.get_str("aadharNo").ok(),
                        &gst_reg_type,
                        &gst_loc,
                        &gst_no,
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
                        &d.get_i32("postgresCrAcc").ok(),
                        &d.get_i32("postgresCrAcc").ok().is_some(),
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
            for coll in ["sales", "credit_notes", "patients"] {
                let command = doc! {
                    "update": coll,
                    "updates": &cus_ref_updates
                };
                mongodb.run_command(command, None).await.unwrap();
            }
            for coll in ["purchases", "debit_notes", "inventories"] {
                let command = doc! {
                    "update": coll,
                    "updates": &ven_ref_updates
                };
                mongodb.run_command(command, None).await.unwrap();
            }
        }
    }
}

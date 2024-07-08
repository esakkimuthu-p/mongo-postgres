use super::*;

pub struct Contact;

impl Contact {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let accounts = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {"postgresAccountType": {"$in":["SUNDRY_DEBTOR", "SUNDRY_CREDITOR"]}},
                find_opts(
                    doc! {"_id": 1, "postgres": 1, "postgresAccountType": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let row = &postgres
            .query_one(
                "select id::bigint from account order by id desc limit 1",
                &[],
            )
            .await
            .unwrap();
        let last_account_id: i64 = row.get(0);
        let _x = postgres
            .execute(
                &format!(
                    "alter sequence public.account_id_seq restart start {}",
                    last_account_id + 1
                ),
                &[],
            )
            .await
            .unwrap();
        // postgres
        //     .execute(
        //         &format!("alter sequence account_id_seq start with 1000"),
        //         &[],
        //     )
        //     .await
        //     .unwrap();
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
        while let Some(Ok(d)) = cur.next().await {
            let account = accounts.iter().find_map(|x| {
                (x.get_object_id("_id").unwrap()
                    == d.get_object_id("creditAccount").unwrap_or_default())
                .then_some(x._get_i32("postgres").unwrap())
            });
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

            let (contact_type, account_type_id) = match d.get_str("contactType").unwrap() {
                "VENDOR" | "PAYABLE" => ("VENDOR", 19),
                "CUSTOMER" | "RECEIVABLE" => ("CUSTOMER", 16),
                "EMPLOYEE" => ("EMPLOYEE", 19),
                _ => panic!("Invalid contact type"),
            };
            if let Some(acc) = account {
                postgres
                .execute(
                        "update account set short_name = $2,pan_no = $3,aadhar_no = $4,gst_reg_type = $5,gst_location_id=$6,gst_no=$7,
                            mobile=$8,alternate_mobile=$9,email=$10,telephone=$11,contact_person=$12,address=$13,
                            city=$14,pincode=$15,state_id=$16,country_id=$17, contact_type = $18 where id = $1",
                    &[
                        &acc,
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
                        &contact_type
                    ],
                )
                .await
                .unwrap();
            } else {
                postgres
                .execute(
                        "INSERT INTO account 
                        (name,short_name,pan_no,aadhar_no,gst_reg_type,gst_location_id,gst_no,
                            mobile,alternate_mobile,email,telephone,contact_person,address,
                            city,pincode,state_id,country_id, contact_type, account_type_id,transaction_enabled) 
                    VALUES 
                        ($1,$2,$3,$4,$5::text,$6,$7,$8,$9,$10,$11,
                        $12,$13,$14,$15,$16,$17,$18::text,$19,false)",
                    &[

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
                        &contact_type,
                        &account_type_id,
                    ],
                )
                .await
                .unwrap();
            }
        }
    }
}

use mongodb::bson::oid::ObjectId;

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
        let mut id: i64 = 0;
        let mut updates = Vec::new();
        let gst_registrations = mongodb
            .collection::<Document>("gst_registrations")
            .find(
                doc! {},
                find_opts(doc! {"gstNo": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let accounts = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {"accountType": "BRANCH_TRANSFER"},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let members = mongodb
            .collection::<Document>("members")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
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
            let branch_members = d
                .get_array("members")
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.as_object_id().unwrap_or_default())
                .collect::<Vec<ObjectId>>();
            let mut m_ids = Vec::new();
            for m in branch_members {
                let mid = members
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == m)
                            .then_some(x._get_i32("postgres").unwrap())
                    })
                    .unwrap();
                m_ids.push(mid)
            }
            let gst_registration = gst_registrations
                .iter()
                .find_map(|x| {
                    (x.get_str("gstNo").unwrap()
                        == d._get_document("gstInfo")
                            .unwrap()
                            .get_str("gstNo")
                            .unwrap())
                    .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            let account = accounts
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("account").unwrap())
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            postgres
                .execute(
                    "INSERT INTO branch 
                        (id,name,mobile,alternate_mobile,email,telephone,contact_person,address,
                            city,pincode,state_id,country_id, gst_registration_id, voucher_no_prefix, 
                            misc, account_id, members) 
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
                        &gst_registration,
                        &d.get_str("voucherNoPrefix").unwrap(),
                        &misc,
                        &account,
                        &m_ids,
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "branches",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}

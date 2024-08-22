use serde_json::json;

use super::*;

pub struct Member;

impl Member {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient, jwt: &String) {
        let mut cur = mongodb
            .collection::<Document>("members")
            .find(
                doc! {"isRoot": false},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"isRoot": -1, "_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut updates = Vec::new();
        let js = serde_json::json!({"jwt_private_key": jwt});
        let perms = vec![
            "doctor__insert",
            "account__insert",
            "bank_beneficiary__select",
            "sale_bill__select",
            "get_gift_voucher__execute",
            "create_sale_bill__execute",
            "get_sale_bill__execute",
            "exchange__select",
            "customer_sale_history__select",
            "vw_recent_sale_bill__select",
            "price_list__select",
            "price_list_condition__select",
            "offer_management__select",
            "doctor__select",
            "e_invoice_proxy__execute",
            "set_e_invoice_irn_details__call",
            "gift_coupon__select",
            "create_stock_deduction__execute",
            "stock_deduction__select",
            "get_stock_deduction__execute",
            "inventory_branch_detail__select",
            "batch__select",
            "batch__update",
            "get_voucher__execute",
            "create_voucher__execute",
            "account_pending__select",
            "inventory__select",
            "account__select",
        ];
        let ui_perms = json!([
            "inv.cus.vw",
            "inv.cus.cr",
            "ac.ac.vw",
            "ac.ac.cr",
            "inv.doc.vw",
            "inv.doc.cr",
            "ac.pmt.cr",
            "inv.sb.vw",
            "inv.sb.cr",
            "inv.stkded.cr"
        ]);
        postgres
            .execute(
                "select set_config('app.env',($1)::json::text,false)",
                &[&js],
            )
            .await
            .unwrap();
        postgres
            .execute(
                "insert into member_role(name, perms, ui_perms)
                values ('custom', $1::text[], $2::json)",
                &[&perms, &ui_perms],
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO member
                        (name,user_id, pass,nick_name,remote_access, is_root, role_id)
                    VALUES ($1, $2, $3, $4, $5, $6, 'custom') returning id",
                    &[
                        &d.get_str("username").unwrap(),
                        &d.get_object_id("user").ok().map(|x| x.to_hex()),
                        &d.get_str("username").unwrap(),
                        &d.get_str("nickName").ok(),
                        &d.get_bool("remoteAccess").unwrap_or_default(),
                        &d.get_bool("isRoot").unwrap_or_default(),
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
        updates.push(doc! {
            "q": { "isRoot": true },
            "u": { "$set": { "postgres": 1} },
        });
        let command = doc! {
            "update": "members",
            "updates": &updates
        };
        mongodb.run_command(command, None).await.unwrap();
    }
}

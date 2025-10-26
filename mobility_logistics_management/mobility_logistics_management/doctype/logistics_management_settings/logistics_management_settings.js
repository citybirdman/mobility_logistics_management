// Copyright (c) 2025, Ahmed Zaytoon and contributors
// For license information, please see license.txt

frappe.ui.form.on("Logistics Management Settings", {
	update_shipping_info_now: async function(frm) {
        frappe.dom.freeze();
        await frappe.call({method: "mobility_logistics_management.mobility_logistics_management.shipping_report_data.Update_shipping_report_data"})
        frappe.msgprint("Shipping information should be updated, check Error log for any errors first.");
        frappe.dom.unfreeze();
        frm.reload_doc();
	},
});

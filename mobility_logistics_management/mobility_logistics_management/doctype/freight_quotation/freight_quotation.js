// Copyright (c) 2025, Ahmed Zaytoon and contributors
// For license information, please see license.txt

frappe.ui.form.on("Freight Quotation", {
	quotation_from(frm) {
        if (frm.doc.quotation_from)
            frm.set_value('quotation_from_doctype', frm.doc.quotation_from)
	},
    freight_terms: function(frm) {
        let terms = {
                Prepaid: ["On Shipment", "Post Shipment"],
                Collect: ["On Arrival", "Post Arrival"]
        }
        frm.fields_dict.payment_terms.set_data(terms[frm.doc.freight_terms] || []);
    }
});

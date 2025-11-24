// Copyright (c) 2025, Ahmed Zaytoon and contributors
// For license information, please see license.txt

frappe.ui.form.on("Freight Quotation", {
	quotation_from(frm) {
        if (frm.doc.quotation_from)
            frm.set_value('quotation_from_doctype', frm.doc.quotation_from)
	},
});

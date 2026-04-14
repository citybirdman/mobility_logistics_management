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
    },
    refresh: function(frm) {
        frm.add_custom_button(__('Link Invoices'), function() {
            let d = new frappe.ui.Dialog({
                title: 'Select Purchase Invoices',
                fields: [
                    {
                        label: 'Invoices',
                        fieldname: 'invoices',
                        fieldtype: 'Table',
                        fields: [
                            {
                                fieldname: 'invoice_id',
                                fieldtype: 'Link',
                                options: 'Purchase Invoice',
                                label: 'ID',
                                in_list_view: 1,
                                columns: 4,
                                // When the ID is selected, fetch the title
                                change: function() {
                                    let grid_row = this.grid_row;
                                    let docname = this.value;
                                    
                                    if (docname) {
                                        frappe.db.get_value('Purchase Invoice', docname, 'title', (r) => {
                                            if (r && r.title) {
                                                // Update the Title field in the same row
                                                grid_row.doc.title = r.title;
                                                grid_row.refresh_field('title');
                                            }
                                        });
                                    }
                                }
                            },
                            {
                                fieldname: 'title',
                                fieldtype: 'Data',
                                label: 'Title',
                                in_list_view: 1,
                                columns: 6,
                                read_only: 1 // Keep it read-only so it's purely informative
                            }
                        ]
                    }
                ],
                primary_action_label: 'Submit',
                primary_action(values) {
                    // values.invoices will be an array of objects: [{invoice_id: "...", title: "..."}, ...]
                    const invoice_ids = values.invoices
                        .filter(row => row.invoice_id) // Ensure we don't send empty rows
                        .map(row => row.invoice_id);

                    frm.call({
                        method: "link_invoices",
                        args: {
                            invoices: invoice_ids,
                            name: frm.doc.name
                        },
                        callback: function(r) {
                            if (!r.exc) {
                                d.hide();
                                frappe.show_alert({message: __('Invoices Linked'), indicator: 'green'});
                            }
                        }
                    });
                }
            });

            d.show();
        });
    }
});

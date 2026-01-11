# Copyright (c) 2025, Ahmed Zaytoon and contributors
# For license information, please see license.txt

import frappe
from frappe.model.naming import make_autoname
from frappe.model.document import Document
from frappe.utils import getdate


class FreightQuotation(Document):
	def autoname(self):
		if not self.valid_from or not self.quoted_by:
			frappe.throw("Both (Valid From) and (Quoted By) must be set before saving.")
		d = getdate(self.valid_from)
		month = d.month
		year = d.year
		prefix = f"{self.quoted_by}-{month}-{year}-"
		self.name = make_autoname(prefix.upper() + ".##")

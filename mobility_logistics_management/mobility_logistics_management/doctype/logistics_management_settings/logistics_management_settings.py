# Copyright (c) 2025, Ahmed Zaytoon and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class LogisticsManagementSettings(Document):
	def validate(self):
		if not 1 <= self.auto_update_job_trigger <= 24:
			frappe.throw("Auto Update Job Trigger must be between 1 and 24 hours.")
		method = "mobility_logistics_management.mobility_logistics_management.shipping_report_data.Update_shipping_report_data"

		if frappe.db.get_value("Scheduled Job Type", {"method": method}):
			frappe.get_doc(
				"Scheduled Job Type",
				{
					"method": method,
				},
			).update(
				{
					"cron_format": f"0 0/{self.auto_update_job_trigger} * * *",
				}
			).save()
		else:
			frappe.get_doc(
				{
					"doctype": "Scheduled Job Type",
					"method": method,
					"cron_format": f"0 0/{self.auto_update_job_trigger} * * *",
					"create_log": True,
					"stopped": False,
					"frequency": "Cron",
				}
			).save()
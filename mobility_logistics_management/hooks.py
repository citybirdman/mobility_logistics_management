app_name = "mobility_logistics_management"
app_title = "Mobility Logistics Management"
app_publisher = "Ahmed Zaytoon"
app_description = "Mobility Logistics Management"
app_email = "citybirdman@gmail.com"
app_license = "mit"

# Apps
# ------------------

# required_apps = []

# Each item in the list will be shown as an app in the apps page
# add_to_apps_screen = [
# 	{
# 		"name": "mobility_logistics_management",
# 		"logo": "/assets/mobility_logistics_management/logo.png",
# 		"title": "Mobility Logistics Management",
# 		"route": "/mobility_logistics_management",
# 		"has_permission": "mobility_logistics_management.api.permission.has_app_permission"
# 	}
# ]

# Includes in <head>
# ------------------

# include js, css files in header of desk.html
# app_include_css = "/assets/mobility_logistics_management/css/mobility_logistics_management.css"
# app_include_js = "/assets/mobility_logistics_management/js/mobility_logistics_management.js"

# include js, css files in header of web template
# web_include_css = "/assets/mobility_logistics_management/css/mobility_logistics_management.css"
# web_include_js = "/assets/mobility_logistics_management/js/mobility_logistics_management.js"

# include custom scss in every website theme (without file extension ".scss")
# website_theme_scss = "mobility_logistics_management/public/scss/website"

# include js, css files in header of web form
# webform_include_js = {"doctype": "public/js/doctype.js"}
# webform_include_css = {"doctype": "public/css/doctype.css"}

# include js in page
# page_js = {"page" : "public/js/file.js"}

# include js in doctype views
# doctype_js = {"doctype" : "public/js/doctype.js"}
# doctype_list_js = {"doctype" : "public/js/doctype_list.js"}
# doctype_tree_js = {"doctype" : "public/js/doctype_tree.js"}
# doctype_calendar_js = {"doctype" : "public/js/doctype_calendar.js"}

# Svg Icons
# ------------------
# include app icons in desk
# app_include_icons = "mobility_logistics_management/public/icons.svg"

# Home Pages
# ----------

# application home page (will override Website Settings)
# home_page = "login"

# website user home page (by Role)
# role_home_page = {
# 	"Role": "home_page"
# }

# Generators
# ----------

# automatically create page for each record of this doctype
# website_generators = ["Web Page"]

# Jinja
# ----------

# add methods and filters to jinja environment
# jinja = {
# 	"methods": "mobility_logistics_management.utils.jinja_methods",
# 	"filters": "mobility_logistics_management.utils.jinja_filters"
# }

# Installation
# ------------

# before_install = "mobility_logistics_management.install.before_install"
# after_install = "mobility_logistics_management.install.after_install"

# Uninstallation
# ------------

# before_uninstall = "mobility_logistics_management.uninstall.before_uninstall"
# after_uninstall = "mobility_logistics_management.uninstall.after_uninstall"

# Integration Setup
# ------------------
# To set up dependencies/integrations with other apps
# Name of the app being installed is passed as an argument

# before_app_install = "mobility_logistics_management.utils.before_app_install"
# after_app_install = "mobility_logistics_management.utils.after_app_install"

# Integration Cleanup
# -------------------
# To clean up dependencies/integrations with other apps
# Name of the app being uninstalled is passed as an argument

# before_app_uninstall = "mobility_logistics_management.utils.before_app_uninstall"
# after_app_uninstall = "mobility_logistics_management.utils.after_app_uninstall"

# Desk Notifications
# ------------------
# See frappe.core.notifications.get_notification_config

# notification_config = "mobility_logistics_management.notifications.get_notification_config"

# Permissions
# -----------
# Permissions evaluated in scripted ways

# permission_query_conditions = {
# 	"Event": "frappe.desk.doctype.event.event.get_permission_query_conditions",
# }
#
# has_permission = {
# 	"Event": "frappe.desk.doctype.event.event.has_permission",
# }

# DocType Class
# ---------------
# Override standard doctype classes

# override_doctype_class = {
# 	"ToDo": "custom_app.overrides.CustomToDo"
# }

# Document Events
# ---------------
# Hook on document methods and events

# doc_events = {
# 	"*": {
# 		"on_update": "method",
# 		"on_cancel": "method",
# 		"on_trash": "method"
# 	}
# }

# Scheduled Tasks
# ---------------

scheduler_events = {
    "cron": {
        "*/60 * * * *": [
            "mobility_logistics_management.mobility_logistics_management.shipping_report_data.Update_shipping_report_data"
        ]
    }
# 	"all": [
# 		"mobility_logistics_management.tasks.all"
# 	],
# 	"daily": [
# 		"mobility_logistics_management.tasks.daily"
# 	],
# 	"hourly": [
# 		"mobility_logistics_management.tasks.hourly"
# 	],
# 	"weekly": [
# 		"mobility_logistics_management.tasks.weekly"
# 	],
# 	"monthly": [
# 		"mobility_logistics_management.tasks.monthly"
# 	],
}

# Testing
# -------

# before_tests = "mobility_logistics_management.install.before_tests"

# Overriding Methods
# ------------------------------
#
# override_whitelisted_methods = {
# 	"frappe.desk.doctype.event.event.get_events": "mobility_logistics_management.event.get_events"
# }
#
# each overriding function accepts a `data` argument;
# generated from the base implementation of the doctype dashboard,
# along with any modifications made in other Frappe apps
# override_doctype_dashboards = {
# 	"Task": "mobility_logistics_management.task.get_dashboard_data"
# }

# exempt linked doctypes from being automatically cancelled
#
# auto_cancel_exempted_doctypes = ["Auto Repeat"]

# Ignore links to specified DocTypes when deleting documents
# -----------------------------------------------------------

# ignore_links_on_delete = ["Communication", "ToDo"]

# Request Events
# ----------------
# before_request = ["mobility_logistics_management.utils.before_request"]
# after_request = ["mobility_logistics_management.utils.after_request"]

# Job Events
# ----------
# before_job = ["mobility_logistics_management.utils.before_job"]
# after_job = ["mobility_logistics_management.utils.after_job"]

# User Data Protection
# --------------------

# user_data_fields = [
# 	{
# 		"doctype": "{doctype_1}",
# 		"filter_by": "{filter_by}",
# 		"redact_fields": ["{field_1}", "{field_2}"],
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_2}",
# 		"filter_by": "{filter_by}",
# 		"partial": 1,
# 	},
# 	{
# 		"doctype": "{doctype_3}",
# 		"strict": False,
# 	},
# 	{
# 		"doctype": "{doctype_4}"
# 	}
# ]

# Authentication and authorization
# --------------------------------

# auth_hooks = [
# 	"mobility_logistics_management.auth.validate"
# ]

# Automatically update python controller files with type annotations for this app.
# export_python_type_annotations = True

# default_log_clearing_doctypes = {
# 	"Logging DocType Name": 30  # days to retain logs
# }


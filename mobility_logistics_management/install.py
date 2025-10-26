import frappe

def after_install():
    create_roles()

def create_roles():
    roles = ["Logistics Manager"]
    for r in roles:
        if not frappe.db.exists("Role", r):
            role = frappe.get_doc({
                "doctype": "Role",
                "role_name": r,
                "desk_access": 1
            })
            role.insert()
            frappe.db.commit()
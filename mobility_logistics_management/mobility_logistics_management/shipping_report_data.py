# %%
import pandas as pd
import frappe 
from rapidfuzz import fuzz, process

@frappe.whitelist()
def get_data():
    columns={
        'order no.': 'title',
        'vol.': 'cntr_vol',
        'type': 'cntr_type',
        'docs':'docs_received',
        'shipping line': 'liner',
        'forwarder': 'forwarder',
        'pol': 'pol',
        'pod': 'pod',
        'etd': 'shipping_date',
        'eta': 'arrival_date',
        'ata': 'arrived',
        'return': 'cntr_returned',
        'f/t': 'free_time',
        'bol no.': 'bol_no',
        'freight/cntr': 'freight_per_cntr',
        'incoterm':'incoterm'
    }
    headers=list(columns.keys())
    new_headers=list(columns.values())
    
    try: 
        link=frappe.db.sql("SELECT value FROM `tabSingles` WHERE doctype = 'Logistics Management Settings' AND field = 'shipping_report_dropbox_shared_uri_path'", as_dict=True)
        path=link[0]['value']
        shipping_file=pd.read_excel(f'https://www.dropbox.com{path}')
        shipping_file.columns = shipping_file.iloc[shipping_file[shipping_file.columns[1]].dropna().index[0]]
        shipping_file.columns=shipping_file.columns.str.lower()
        shipping_file = shipping_file.iloc[shipping_file[shipping_file.columns[1]].dropna().index[0]+1:].reset_index(drop=True)
        master_data=pd.read_excel(f'https://www.dropbox.com{path}',sheet_name='master_data')
        shipping_file=shipping_file[~shipping_file.ETD.isna()].reset_index(drop=True)
        shipping_file = shipping_file[headers]
        shipping_file=shipping_file.rename(columns=columns) # type: ignore
        master_data=master_data.fillna('') 
        shipping_file=shipping_file.fillna('') 
    except Exception as e:
        frappe.throw("Shipping Report Dropbox Shared URI Path not set in Company settings.\n {%s}".format(e))
        return {
            'status': 'error'}
      
    try:      
        for idx, row in shipping_file.iterrows():
            port_name = row[new_headers[6]]  # POL column
            best_match = None
            best_ratio = 0
            
            for _, row2 in master_data.iterrows():
                port_name2 = row2['pol'] 
                ratio = fuzz.ratio(port_name.lower().strip().split('-')[0], port_name2.lower().strip().split('-')[0])
                
                if ratio > best_ratio:  
                    best_ratio = ratio
                    best_match = port_name2
            
            if best_ratio > 70:  
                shipping_file.at[idx, new_headers[6]] = best_match
                # data1.at[idx, 'fuzzy%'] = best_ratio
            else:
                frappe.throw(f"This port <b>{port_name}</b> does not exist in Port of Loading Master. Please add it accordingly.")
                       
        for idx, row in shipping_file.iterrows():
            shipping_line = row[new_headers[4]] # Shipping Line column
            best_match = None
            best_ratio = 0
            for _, row2 in master_data.iterrows():
                shipping_line2 = row2['liner']
                ratio = fuzz.ratio(shipping_line.lower().strip().split('-')[0], shipping_line2.lower().strip().split('-')[0])
                if ratio > best_ratio:  
                    best_ratio = ratio
                    best_match = shipping_line2
            
            if best_ratio > 70:  
                shipping_file.at[idx, new_headers[4]] = best_match
                # shipping_file.at[idx, 'fuzzy%'] = best_ratio
            else:
                frappe.throw(f"This liner <b>{shipping_line}</b> does not exist in Shipping Line Master. Please add it accordingly.")
        
        for idx, row in shipping_file.iterrows():
            forwarder = row[new_headers[5]] # Forwarder column
            best_match = None
            best_ratio = 0
            for _, row2 in master_data.iterrows():
                forwarder2 = row2['forwarder']
                ratio = fuzz.ratio(forwarder.lower().strip().split('-')[0], forwarder2.lower().strip().split('-')[0])
                if ratio > best_ratio:  
                    best_ratio = ratio
                    best_match = forwarder2
            
            if best_ratio > 70:  
                shipping_file.at[idx, new_headers[5]] = best_match
            else:
                frappe.throw(f"This forwarder <b>{forwarder}</b> does not exist in Freight Forwarder Master. Please add it accordingly.")    
                
        for index, row in shipping_file.iterrows():
            if row[new_headers[3]]=='#':
                shipping_file.at[index, new_headers[3]] = 1
            else:
                shipping_file.at[index, new_headers[3]] = 0
            
            if row[new_headers[11]]=='#':
                shipping_file.at[index, new_headers[11]] = 1
            else:
                shipping_file.at[index, new_headers[11]] = 0   
                
        for index ,row in shipping_file.iterrows():
                if row[new_headers[9]]=='Arrived':
                    shipping_file.at[index, new_headers[9]] = row[new_headers[10]]
                    shipping_file.at[index, new_headers[10]] = 1
                else:
                    shipping_file.at[index, new_headers[10]] = 0


        for index, row in shipping_file.iterrows():
                cell = row[new_headers[14]]
                try:
                    value_in_parens = cell.split('(')[1].split(')')[0]
                    if value_in_parens.isdigit():
                        shipping_file.at[index, new_headers[14]] = value_in_parens
                    else:
                        shipping_file.at[index, new_headers[14]] = 0
                except (IndexError, AttributeError):
                        shipping_file.at[index, new_headers[14]] = 0
       

        return shipping_file.to_dict(orient='records')
    
    except Exception as e:
        frappe.throw(f"Error while processing the shipping data.\n {e}")  
        return {
            'status': 'error'
        }


@frappe.whitelist()
def Update_shipping_report_data():
    settings = frappe.get_doc("Logistics Management Settings")
    try:
        logs = get_data()
        for log in logs:
            frappe.db.set_value("Purchase Invoice", {"title": log['title']}, log)
        settings.latest_successful_sync_date = settings.latest_sync_date = frappe.utils.now_datetime()
        settings.error_log = ""
        settings.save()
    except Exception as e:
        frappe.log_error(title="Error while fetching shipping report data.", message=f"{e}")
        settings.latest_sync_date = frappe.utils.now_datetime()
        settings.error_log = e
        settings.save()
# %%

# substitute keys in a path with corresponding path_strings from a class object 
# remove from the path if it doesn't exist

def sub_path(class_object,path_string):
    if hasattr(class_object, 'Year'):
        path_string = path_string.replace('YEAR',str(class_object.Year))
    else:
        path_string = path_string.replace('YEAR','')
    
    if hasattr(class_object, 'Month'):
        path_string = path_string.replace('MONTH',str(class_object.Month))
    else:
        path_string = path_string.replace('MONTH','')
    
    if hasattr(class_object, 'SiteID'):
        path_string = path_string.replace('SITEID',str(class_object.SiteID))
    else:
        path_string = path_string.replace('SITEID','')
    
    if hasattr(class_object, 'dateStr'):
        path_string = path_string.replace('DATE',str(class_object.dateStr))
    else:
        path_string = path_string.replace('DATE','')
    
    return(path_string)
def codes_to_sql_where(col_name, code_list):
    where = ""
    i = 0
    for code in code_list:
        if i == 0:
            where = where + f"{col_name} = '{code}'"
        else:
            where = where + f" OR {col_name} = '{code}'"
        i+=1
    return where

def search_terms_to_df(search_terms, col_name, df_name):
    counter = 0
    for i in search_terms:
        if counter == 0: 
            terms = i
            counter = counter + 1
        else:
            terms = terms + "|" + i
    df = df_name[df_name[col_name].str.contains(terms)]
    return df
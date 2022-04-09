
def qa_data_set(data_set_name, data_set):
    if data_set.empty:
        qa_result = 0
        print('ERROR: ' + data_set_name + ' dataset is empty')
    else:
        qa_result = 1
    return qa_result
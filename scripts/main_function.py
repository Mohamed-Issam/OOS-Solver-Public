# Function to check if activity is out of sequence based on logic rules
def check_oos(task_id, df_table, df_relation):
    global df_import
    new_rows = []

    # Get all predecessor relationships for this task
    pred_relations = df_relation[(df_relation['Successor'] == task_id) & (df_relation['Activity Status'] != 'Completed')]

    if pred_relations.empty:
        return False, ''

    # Get successor details from df_table
    succ = df_table[df_table['TASK_ID'] == task_id].iloc[0]
    succ_start = pd.to_datetime(succ['Actual Start']) if pd.notna(succ['Actual Start']) else None
    succ_finish = pd.to_datetime(succ['Actual Finish']) if pd.notna(succ['Actual Finish']) else None
    succ_status = df_table[df_table['TASK_ID'] == task_id]['Activity Status'].iloc[0]

    for _, relation in pred_relations.iterrows():
        pred_start = pd.to_datetime(relation['Pred Actual Start']) if pd.notna(relation['Pred Actual Start']) else None
        pred_finish = pd.to_datetime(relation['Pred Actual Finish']) if pd.notna(relation['Pred Actual Finish']) else None
        rel_type = relation['Relationship Type']
        pred_activity = relation['Predecessor']
        pred_status = relation['Activity Status']

        # for lookups
        succ_true_name = lookup_activity_name(task_id, df_table)
        pred_true_name = lookup_activity_name(pred_activity, df_table)

        # Skip if successor hasn't started yet
        if succ_start is None:
            continue

        # Case 1: Successor starts before Predecessor finishes (FS)
        if rel_type == 'FS' and pred_finish is not None and succ_start < pred_finish:
            new_rows = [
                        {
                            'pred_task': pred_true_name,                                 # Predecessor
                            'task_id': succ_true_name,                                   # Successor
                            'pred_type': rel_type,                                       # Relationship
                            'PREDTASK__status_code': pred_status,                        # Predecessor Status
                            'TASK__status_code': succ_status,                            # Successor Status
                            'delete_record_flag': 'd'                                    # delete flag
                        },
                        {
                            'pred_task': pred_true_name,                                 # Predecessor
                            'task_id': succ_true_name,                                   # Successor
                            'pred_type': 'SS',                                           # Relationship Correction
                            'PREDTASK__status_code': pred_status,                        # Predecessor Status
                            'TASK__status_code': succ_status,                            # Successor Status
                            'delete_record_flag': ''
                        }
                        ]
            df_import = pd.concat([df_import, pd.DataFrame(columns=df_import.columns.tolist(), data=new_rows)], ignore_index=True)
            return True, 'Case 1'

        # Case 2: Successor starts before Predecessor starts (FS or SS)
        if pred_start is None and rel_type in ('FS', 'SS'):
            new_rows = [
                        {
                            'pred_task': pred_true_name,                                 # Predecessor
                            'task_id': succ_true_name,                                   # Successor
                            'pred_type': rel_type,                                       # Relationship
                            'PREDTASK__status_code': pred_status,                        # Predecessor Status
                            'TASK__status_code': succ_status,                            # Successor Status
                            'delete_record_flag': 'd'                                    # delete flag
                        },
                        {
                            'pred_task': pred_true_name,                                 # Predecessor
                            'task_id': succ_true_name,                                   # Successor
                            'pred_type': 'FF',                                           # Relationship Correction
                            'PREDTASK__status_code': pred_status,                        # Predecessor Status
                            'TASK__status_code': succ_status,                            # Successor Status
                            'delete_record_flag': ''
                        }
                        ]
            df_import = pd.concat([df_import, pd.DataFrame(columns=df_import.columns.tolist(), data=new_rows)], ignore_index=True)
            return True, 'Case 2'

        # Case 3: Successor finishes while Predecessor still in progress (FS or FF)
        if succ_finish is not None and pred_finish is None and pred_start is not None and rel_type in ('FS', 'FF'):
            new_rows = [
                        {
                            'pred_task': pred_true_name,                                 # Predecessor
                            'task_id': succ_true_name,                                   # Successor
                            'pred_type': rel_type,                                       # Relationship
                            'PREDTASK__status_code': pred_status,                        # Predecessor Status
                            'TASK__status_code': succ_status,                            # Successor Status
                            'delete_record_flag': 'd'                                    # delete flag
                        },
                        {
                            'pred_task': pred_true_name,                                 # Predecessor
                            'task_id': succ_true_name,                                   # Successor
                            'pred_type': 'SS',                                           # Relationship Correction
                            'PREDTASK__status_code': pred_status,                        # Predecessor Status
                            'TASK__status_code': succ_status,                            # Successor Status
                            'delete_record_flag': ''
                        }
                        ]
            df_import = pd.concat([df_import, pd.DataFrame(columns=df_import.columns.tolist(), data=new_rows)], ignore_index=True)
            return True, 'Case 3'

        # Case 4: Successor finishes before Predecessor starts (FS, FF, SS)
        if succ_finish is not None and pred_start is None and rel_type in ('FS', 'FF', 'SS'):
            # make sure that there are other predecessors to successor activity before deleting predecessor relation
            if not df_relation[(df_relation['Successor'] == task_id) & (df_relation['Predecessor'] != pred_activity)].empty:
                new_rows = [
                            {
                                'pred_task': pred_true_name,                                 # Predecessor
                                'task_id': succ_true_name,                                   # Successor
                                'pred_type': rel_type,                                       # Relationship Correction
                                'PREDTASK__status_code': pred_status,                        # Predecessor Status
                                'TASK__status_code': succ_status,                            # Successor Status
                                'delete_record_flag': 'd'
                            }
                            ]
                df_import = pd.concat([df_import, pd.DataFrame(columns=df_import.columns.tolist(), data=new_rows)], ignore_index=True)
            else:
                print(f'Need to review hard logic with Predecessor {pred_true_name} and Successor {succ_true_name}')
            return True, 'Case 4'

    return False, ''

# Check OOS for each task in df_tablelookup_activity_name(pred_activity, df_table)
df_table[['Is_OOS', 'Case']] = df_table['TASK_ID'].apply(lambda x: pd.Series(check_oos(x, df_table, df_relation)))


# Print summary of OOS activities
print("\nOut of Sequence Activities Summary:")
df_table['Is_OOS'].value_counts()
from mint.api.api_curd import *


def test_create_tree():


    jo = {
        "root": "project",
        "values": {
            "project": [
                {
                    "name": "project1",
                    'st_date': dt(2025, 1, 1),
                    'notional': 0,
                    'type_ledger_detail': '其他',
                    'type_predict': '其他',
                    'type_scene': '不适用',
                    'project_credit_name': '未指定',
                    'off_balance_sheet_type': '不适用',
                    'annual_days': 365,
                    'is_fed_project': 0,
                    'project_structured_info': '不适用'
                },
            ],

            "project_level": [
                {"name": 'p1-level1', "project_name": 'project1'},
                {"name": 'p1-level2', "project_name": 'project1'},
                {"name": 'p1-level3', "project_name": 'project1'},
            ]
        }
    }
    create_tree(jo=jo)


if __name__ == '__main__':
    test_create_tree()
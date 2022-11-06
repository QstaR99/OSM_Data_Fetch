import pandas as pd
import osmapi
import xmltodict
import requests
from dateutil.parser import parse
from datetime import datetime

USER_INFO_API_URL = "https://www.openstreetmap.org/api/0.6/user/"
LAST_100_CHANGESETS_API_URL = "http://api.openstreetmap.org/api/0.6/changesets?display_name="


def parse_timestamp_feature(ts):
    try:
        return int(parse(ts).timestamp())
    except TypeError:
        return 0


def get_user_creation_date(user_id):
    try:
        user_info = requests.get(USER_INFO_API_URL+str(user_id)).content
        data_dict = xmltodict.parse(user_info)
        return data_dict['osm']['user']['@account_created']

    except Exception:
        return 0


def changeset_features(changeset_id):

    api_instance = osmapi.OsmApi()

    features = {}

    data= api_instance.ChangesetGet(changeset_id)
    num_changes = data['changes_count']

    if int(num_changes) == 0: return False

    if 'min_lat' in data:
        features["min_lat"], features["max_lat"] = data['min_lat'], data['max_lat']
        features["min_lon"], features["max_lon"] = data['min_lon'], data['max_lon']

    else:
        features["min_lat"], features["max_lat"] = 0, 0
        features["min_lon"], features["max_lon"] = 0, 0

    features["closed"] = data['closed_at'].isoformat()

    if 'comment' in data['tag']:
        features["comment_len"] = len(str(data['tag']['comment']).strip())

    else:
        features["comment_len"] = 0

    api_instance.close()
    return {"features": features, "raw": data}


def analyze_changeset(changeset_id: int):

    api_instance = osmapi.OsmApi()

    data = api_instance.ChangesetDownload(changeset_id)

    stat = {
        "create": 0,
        "modify": 0,
        "delete": 0,
        "node": 0,
        "way": 0,
        "relation": 0
    }

    if len(data) == 0:
        return stat, []
    
    edit_matrix = list()
    
    for edit in data:

        stat[edit['action']] += 1
        stat[edit['type']] += 1
        data = {"changeset": changeset_id}
        data["type"] = edit['type']
        data["action"] = edit['action']
        data['version'] = edit['data']['version']
        data['id'] = edit['data']['id']
        data['timestamp'] = edit['data']['timestamp']
        data['uid'] = edit['data']['uid']
        edit_matrix.append(data)
    
    api_instance.close()
    return stat, edit_matrix


def user_previous_data(changeset_close_date, user):

    api_call = LAST_100_CHANGESETS_API_URL+user+"&time=2001-01-01," + changeset_close_date
    data = requests.get(api_call).content
    data = xmltodict.parse(data)

    ret_stat = {
        "creates": 0,
        "modifies": 0,
        "deletes": 0,
        "nodes": 0,
        "ways": 0,
        "relations": 0
    }

    checks = []
    checks_c = []
    while("changeset" in data['osm'] and len(data['osm']['changeset']) > 1):
        changesets = data['osm']['changeset']
        if type(changesets) == list:
            changesets.pop(0)

        for changeset in changesets:
            if type(changeset) != dict:
                continue
            if changeset['@id'] in checks_c: continue
            checks_c.append(changeset['@id'])
            stat, _ = analyze_changeset(int(changeset['@id']))
            ret_stat['creates'] += stat['create']
            ret_stat['modifies'] += stat['modify']
            ret_stat['deletes'] += stat['delete']
            ret_stat['nodes'] += stat['node']
            ret_stat['ways'] += stat['way']
            ret_stat['relations'] += stat['relation']
            prev_changeset = changeset

        if prev_changeset['@id'] in checks:
            break
        checks.append(prev_changeset['@id'])
        changeset_close_date = prev_changeset['@closed_at']
        api_call = LAST_100_CHANGESETS_API_URL+user+"&time=2001-01-01," + changeset_close_date
        data = requests.get(api_call).content
        data = xmltodict.parse(data)
    print(ret_stat["creates"] + ret_stat['modifies'] + ret_stat['deletes'])
    return ret_stat


def changeset_and_edit_features(changeset_id: int) -> list:

    print(f"-------------------------------------- CHANGESET {changeset_id} --------------------------------------")

    ret = changeset_features(changeset_id)
    features = ret['features']
    user = ret['raw']['user']

    stat, edit_matrix = analyze_changeset(changeset_id)
    features.update(stat)

    print(user_previous_data(features['closed'], user))

    return features, edit_matrix


if __name__ == "__main__":

    labels = pd.read_csv('D:\ovid\Ovid-main\labels\ovid_labels.tsv', sep='\t')

    for i in range(len(labels)):
        id = int(labels.iloc[i]['changeset'])

        features = changeset_and_edit_features(id)
        print(features[0])
        # for y in features[1]:
        #     print(y)
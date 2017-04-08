import numpy as np
import pandas as pd
import pytest

@pytest.mark.parametrize('fp', [None, 'a'])
def test_insert_fingerprint_fail(fp, traxit_db):
    with pytest.raises(TypeError):
        traxit_db.insert_fingerprint(fp, 'trackid')


def test_get_fp_ids_empty(fingerprints, traxit_db):
    for track_id, fp in fingerprints:
        traxit_db.insert_fingerprint(fp, track_id)
    traxit_db.delete_all()
    fp_ids = traxit_db.get_fp_ids()
    assert set() == set(fp_ids)


def test_get_fp_ids(fingerprints, traxit_db):
    for track_id, fp in fingerprints:
        traxit_db.insert_fingerprint(fp, track_id)
    fp_ids = traxit_db.get_fp_ids()
    assert len(fingerprints) == len(fp_ids)
    assert set([track_id for track_id, fp in fingerprints]) == set(fp_ids)


def test_query_fingerprint(fingerprints, traxit_db):
    fps = fingerprints

    for track_id, fp in fps:
        traxit_db.insert_fingerprint(fp, track_id)
    for track_id, fp in fps:
        fp_bis = traxit_db.query_fingerprint(track_id)
        assert (fp == fp_bis).all().all()


def test_query_keys(fingerprints, traxit_db):
    for track_id, fp in fingerprints:
        traxit_db.insert_fingerprint(fp, track_id)

    track_id1, track_id2 = fingerprints[0][0], fingerprints[1][0]

    trackid_t_res_all = traxit_db.query_keys(
        {1, 2, 3, 4},
        [track_id1, track_id2]
        )

    assert trackid_t_res_all[track_id1][1]['index'].tolist() == [0, 3]
    assert trackid_t_res_all[track_id1][1]['detuning'].tolist() == [20, 50]
    assert trackid_t_res_all[track_id2][1]['index'].tolist() == [0, 3]
    assert trackid_t_res_all[track_id2][1]['detuning'].tolist() == [20, 50]

    assert trackid_t_res_all[track_id1][2]['index'].tolist() == [1]
    assert trackid_t_res_all[track_id1][2]['detuning'].tolist() == [30]
    assert trackid_t_res_all[track_id2][2]['index'].tolist() == [1]
    assert trackid_t_res_all[track_id2][2]['detuning'].tolist() == [30]

    assert trackid_t_res_all[track_id2][3]['index'].tolist() == [2]
    assert trackid_t_res_all[track_id2][3]['detuning'].tolist() == [40]

    assert trackid_t_res_all[track_id1][4]['index'].tolist() == [2]
    assert trackid_t_res_all[track_id1][4]['detuning'].tolist() == [40]


def test_keys_count(traxit_db):
    fp1 = pd.DataFrame([{'key': i, 'detuning': 0}
                        for i in np.random.randint(0, 50000, size=1000)])
    fp2 = pd.DataFrame([{'key': i, 'detuning': 0}
                        for i in np.random.randint(0, 50000, size=1000)])
    traxit_db.insert_fingerprint(fp1, '1')
    traxit_db.insert_fingerprint(fp2, '2')
    assert traxit_db.keys_count() == 2


def test_query_track_ids(traxit_db):
    keys = np.random.randint(0, 50000, size=1000).tolist()
    fp1 = pd.DataFrame([{'key': key, 'detuning': 0}
                        for key in keys])
    fp2 = pd.DataFrame([{'key': key, 'detuning': 0}
                        for key in keys[:200]])
    traxit_db.insert_fingerprint(fp1, '1')
    traxit_db.insert_fingerprint(fp2, '2')
    track_ids = traxit_db.query_track_ids(keys, 1, quality=10)
    assert track_ids == ['1']
    track_ids = traxit_db.query_track_ids(keys, 2, quality=10)
    assert track_ids == ['1', '2']


def test_many_keys(traxit_db):
    keys = np.random.randint(0, 50000, size=1000)
    unique_keys = list(set(keys))
    fp1 = pd.DataFrame({'key': keys, 'detuning': 0})
    fp2 = pd.DataFrame({'key': keys, 'detuning': 0})
    traxit_db.insert_fingerprint(fp1, '1')
    traxit_db.insert_fingerprint(fp2, '2')
    queried_keys = traxit_db.query_keys(unique_keys, ['1', '2'])
    assert set(queried_keys['1']) == set(unique_keys)
    assert set(queried_keys['2']) == set(unique_keys)


def test_is_ingested_fingerprint(fingerprint, traxit_db):
    track_id, fp = fingerprint
    traxit_db.insert_fingerprint(fp, track_id)
    assert traxit_db.is_ingested_fingerprint(track_id)
    traxit_db.delete_fingerprint(track_id)
    assert not traxit_db.is_ingested_fingerprint(track_id)

from five_to_eight.api.call import ice_breaking, get_key, gen_url, req, req2list, list2df, save2df
import pandas as pd

def test_ice_breaking():
    img=ice_breaking()
    print(img)

def test_get_url():
    url=gen_url(20220101, { "multiMovieYn": "Y"})
    print(url)

    assert len(url) > 0

def test_get_key():
    key=get_key()
    print(key)

    assert len(key) > 0

def test_req():
    code, data = req(20220101, { "multiMovieYn": "Y"})
    print(data)
    assert code == 200

def test_req2list():
    l = req2list()
    assert len(l) > 0
    v = l[0]
    assert 'rnum' in v.keys()
    assert v['rnum'] == '1'

def test_list2df():
    df = list2df()
    assert isinstance(df, pd.DataFrame)
    assert 'rnum' in df.columns
    assert 'openDt' in df.columns
    assert 'movieNm' in df.columns
    assert 'audiAcc' in df.columns

def test_save2df():
    df = save2df()
    assert isinstance(df, pd.DataFrame)
    assert 'load_dt' in df.columns
    assert len(df) == 10

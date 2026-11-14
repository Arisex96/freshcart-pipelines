# Import Mage's decorators only if they aren't already loaded
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# transformer receives load_iris's 150-row table as 'df'
# It only reports what it sees and passes the table on unchanged to prepare_data
@transformer
def validate(df, *args, **kwargs):
    print('Missing values:', int(df.isnull().sum().sum()), '| Classes:', df['target'].nunique())
    return df


# Runs right after validate finishes
# Stops the pipeline if any value is missing or the number of species is not 3
@test
def test_no_missing_and_3_classes(output, *args):
    assert output.isnull().sum().sum() == 0, 'Iris data has missing values'
    assert output['target'].nunique() == 3, 'Iris data must have exactly 3 classes'

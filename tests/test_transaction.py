from minorm import transaction


def test_rollback(test_model):

    with transaction.atomic():
        test_model.qs.create(name="foo", age=10)

        transaction.rollback()
        test_model.qs.create(name="bar", age=11)

    results = test_model.qs.all()
    assert len(results) == 1
    assert results[0].name == "bar"


def test_commit(test_model):
    db = test_model._meta.db

    with transaction.atomic():
        test_model.qs.create(name="foo", age=10)
        transaction.commit()

        test_model.qs.create(name="bar", age=11)
        db._connection.rollback()

    results = test_model.qs.all()
    assert len(results) == 1
    assert results[0].name == "foo"

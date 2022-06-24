class Fruit:
    NAME = None

    @classmethod
    def is_match(cls, name: str) -> bool:
        b = False
        if cls.NAME:
            if cls.NAME == name:
                b = True
        else:
            raise Exception("NAME is not set")
        return b


class Apple(Fruit):
    NAME = "apple"


class Orange(Fruit):
    pass


print(Apple.is_match("apple"))

try:
    print(Orange.is_match("foo"))
except Exception as ex:
    print(ex)

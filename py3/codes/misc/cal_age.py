from datetime import datetime


def age(birth_date_str: str, base_date_str: str) -> int:
    birth_date = datetime.strptime(birth_date_str, "%Y%m%d")
    base_date = datetime.strptime(base_date_str, "%Y%m%d")
    year_diff = base_date.year - birth_date.year
    if year_diff <= 0:
        age = 0
    else:
        if (base_date.month, base_date.day) > (birth_date.month, birth_date.day):
            age = year_diff
        else:
            age = year_diff - 1
    return age


if __name__ == "__main__":
    print(age("20220624", "20220624"))
    print(age("19780601", "20220624"))
    print(age("19780701", "20220624"))

read_token = "t.NxzOivbLZmd-IPkYvau9INqULxzZoVCYOdfjlnjDapkdqxpkq2D2WmeAO6t0oc_a3QAhszq7i4WVlJuJheyf5g"
real_token = "t.T1OlPrEgbB1oXPH_qn61bTBvqOkgyGg3zLIYekURUFGaVrs5wthpU8X2RKXRYsl-35TVPSNiwCuji2qJHELwmg"


def get_token(trade):
    if trade:
        return real_token
    else:
        return read_token

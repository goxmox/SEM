from pomegranate.hmm import DenseHMM
from pomegranate.gmm import GeneralMixtureModel
from pomegranate.distributions import Normal, DiracDelta
from hmmlearn.hmm import GaussianHMM
import pandas as pd
import torch


class HMMPomegranate(DenseHMM):
    def __init__(
            self,
            *args,
            num_of_improvements=1,
            abs_tol=False,
            normal_states=3,
            zero_states=0,
            **kwargs
    ):
        if 'distributions' not in kwargs.keys():
            kwargs['distributions'] = [Normal(covariance_type='full') for _ in range(normal_states)] \
                                      + [DiracDelta(alphas=[1, 1], frozen=True) for _ in range(zero_states)]

        super().__init__(*args, **kwargs)
        self._num_of_improvements = num_of_improvements
        self._abs_tol = abs_tol
        self.forward_prob: torch.Tensor = None

        self.name += f'(normal_states={normal_states},zero_states={zero_states})'

    def save_model(self):
        data = {
            'edges': self.edges,
            'starts': self.starts,
            'ends': self.ends,
            'forward': self.forward_prob,
            'distributions': [(dist.means, dist.covs) for dist in self.distributions]
        }

        return data

    def load_model(self, data):
        self.edges = data['edges']
        self.ends = data['ends']
        self.starts = data['starts']
        self.forward_prob = data['forward']
        self.state_to_order: list = {}

        self.distributions = None
        self.add_distributions([Normal(
                means=moments[0],
                covs=moments[1],
                covariance_type='full'
            ) for moments in data['distributions']])

    def forward(self, X=None, emissions=None, priors=None):
        f = super().forward(X=X, emissions=emissions, priors=priors)
        self.forward_prob = f[0, -1, :]

        return f

    def fit(self, X, sample_weight=None, priors=None, pretrain_gmm=False):
        if pretrain_gmm:
            GeneralMixtureModel(distributions=self.distributions, tol=100, verbose=True, inertia=0.9).fit(X.to_numpy())

        if sample_weight is not None:
            sample_weight = sample_weight.reshape(-1)

        X = X.reshape(1, X.shape[0], X.shape[1])

        super().fit(X, sample_weight=sample_weight, priors=priors)

        return self

    def forecast(self, h: int = 1):
        f = torch.log(
            torch.exp(self.forward_prob - torch.logsumexp(self.forward_prob, dim=0))
            @ torch.linalg.matrix_power(torch.exp(self.edges), h)
        )

        return f

    def update(self, X: pd.DataFrame):
        X = X.to_numpy()

        for t in range(X.shape[0]):
            p = torch.Tensor([d.log_probability(X[t, :].reshape(1, -1)) for d in self.distributions])
            logp = torch.logsumexp(self.forward_prob, dim=0)
            self.forward_prob = torch.log(
                torch.exp(self.forward_prob - logp).reshape(1, -1) @ torch.exp(self.edges)
            ) + logp + p
            self.forward_prob = self.forward_prob.reshape(-1)

        return self.forward_prob

class HMMLearn(GaussianHMM):
    def __init__(
            self,
            n_components=2,
            covariance_type='full',
            **kwargs
    ):
        super().__init__(n_components=n_components, covariance_type=covariance_type, **kwargs)

        self.name = f'HMMLearn(n_components={n_components},covariance_type={covariance_type})'

    def save_model(self):
        return self
import pandas as pd
import sys
from sklearn.cross_validation import StratifiedKFold
from bayes_opt import BayesianOptimization
from sklearn.cross_validation import cross_val_score
from xgboost import XGBClassifier
import xgboost

class XGBModel(object):
    def __init__(self, train, feature_names, label_name):
        self.train = train
        self.feature_names = feature_names
        self.label_name = label_name
        self.parameters = {}
        self.model = None

    def optimize_hyperparams(self, initial_params={}, verbose=1, init_points=2, n_iter=3):
        data = self.train[self.feature_names]
        target = self.train[self.label_name]

        def xgbcv(nr_classifiers, learning_rate, max_depth, min_child_weight, subsample, colsample_bytree, gamma,
                  reg_lambda):
            nr_classifiers = int(nr_classifiers)
            max_depth = int(max_depth)
            min_child_weight = int(min_child_weight)
            return cross_val_score(XGBClassifier(learning_rate=learning_rate, n_estimators=nr_classifiers,
                                                 gamma=gamma, subsample=subsample, colsample_bytree=colsample_bytree,
                                                 nthread=1, scale_pos_weight=1, reg_lambda=reg_lambda,
                                                 min_child_weight=min_child_weight, max_depth=max_depth),
                                   data, target, 'accuracy', cv=5).mean()

        params = {
            'nr_classifiers': (50, 2500),
            'learning_rate': (0.01, 0.4),
            'max_depth': (5, 20),
            'min_child_weight': (2, 20),
            'subsample': (0.5, 0.9),
            'colsample_bytree': (0.5, 0.99),
            'gamma': (1., 0.01),
            'reg_lambda': (0., 1.)
        }

        xgbBO = BayesianOptimization(xgbcv, params, verbose=verbose)
        if initial_params != {}: xgbBO.explore(initial_params)
        xgbBO.maximize(init_points=init_points, n_iter=n_iter, n_restarts_optimizer=100)

        best_params = xgbBO.res['max']['max_params']
        self.parameters = best_params
        return self.parameters

    def construct_model(self):
        data = self.train[self.feature_names]
        target = self.train[self.label_name]

        best_nr_classifiers = int(self.parameters['nr_classifiers'])
        best_max_depth = int(self.parameters['max_depth'])
        best_min_child_weight = int(self.parameters['min_child_weight'])
        best_colsample_bytree = self.parameters['colsample_bytree']
        best_subsample = self.parameters['subsample']
        best_reg_lambda = self.parameters['reg_lambda']
        best_learning_rate = self.parameters['learning_rate']
        best_gamma = self.parameters['gamma']

        self.model = XGBClassifier(learning_rate=best_learning_rate, n_estimators=best_nr_classifiers,
                                   gamma=best_gamma, subsample=best_subsample, colsample_bytree=best_colsample_bytree,
                                   nthread=1, scale_pos_weight=1, reg_lambda=best_reg_lambda,
                                   min_child_weight=best_min_child_weight, max_depth=best_max_depth)
        self.model.fit(data, target)
        return self.model

    def model_to_json(self):
        self.model._Booster.save_model('201612301448.model')

    def predict_proba(self, test_vector):
        return self.model.predict_proba(test_vector)

    def predict(self, test_vector):
        return self.model.predict(test_vector)

    def cross_validate(self):
        pass
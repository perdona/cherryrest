#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.expression import true
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from . import models
from cherrypy import NotFound
import uuid
import copy

Base = models.Base


def model_query(session, model):
    return session.query(model)


class Database(object):
    def __init__(self, type, path, autocommit=True):
        self.db_path = path
        self.type = type
        self.engine = create_engine(path)
        Base.metadata.bind = self.engine
        DBSession = sessionmaker(bind=self.engine, autocommit=autocommit,)
        self.session = DBSession()
        self.create()

    @staticmethod
    def sort(model, query, sort_key, sort_dir):
        if sort_dir.lower() not in ('desc', 'asc'):
            return
        sort_attr = getattr(model, sort_key)
        sort_method = getattr(sort_attr, sort_dir.lower())
        return query.order_by(sort_method())

    def create(self, force=False):
        if not os.path.exists(self.db_path) or force:
            Base.metadata.create_all(self.engine)

    def add(self, obj):
        self.session.add(obj)
        self.flush()

    def commit(self):
        self.session.commit()

    def flush(self):
        self.session.flush()

    def soft_delete(self, obj):
        obj.soft_delete(self.session)
        self.flush()

    def get_type(self):
        return self.type


    def get_model_by_id2(self, model, id2):
        query = model_query(self.session, model)
        query = query.filter(model.id2 == id2)
        result = query.first()
        if not result:
            raise NotFound()
        return result


    def get_tasks(self, **kwargs):
        limit = kwargs.get('limit', 0)
        offset = kwargs.get('offset', 0)
        sort_key = kwargs.get('sort_key', 'created_at')
        sort_dir = kwargs.get('sort_dir', 'desc')
        query = model_query(self.session, models.Task)
        query = Database.sort(models.Task, query, sort_key, sort_dir)
        worker_id = kwargs.get('worker_id', -1)
        worker_name = kwargs.get('worker_name', None)
        task_name = kwargs.get('name')
        policy_id = kwargs.get('policy_id')
        policy_name = kwargs.get('policy_name')
        if 'deleted' not in kwargs:
            query = query.filter(models.Task.deleted == 'False')
        if worker_name:
            w = self.get_worker_by_name(worker_name)
            worker_id = w.id
        if policy_name:
            p = self.get_policy_by_name(policy_name)
            policy_id = p.id
        if worker_id >= 0:
            query = query.filter(models.Task.worker_id == worker_id)
        if policy_id:
            query = query.filter(models.Task.policy_id == policy_id)
        if task_name:
            query = query.filter(models.Task.name == task_name)
        total = query.count()
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query.all(), total

    def get_tasks_all(self, **kwargs):
        limit = kwargs.get('limit', 0)
        offset = kwargs.get('offset', 0)
        sort_key = kwargs.get('sort_key', 'created_at')
        sort_dir = kwargs.get('sort_dir', 'desc')
        worker_id = kwargs.get('worker_id', -1)
        worker_name = kwargs.get('worker_name', None)
        task_name = kwargs.get('name')
        if worker_name:
            w = self.get_worker_by_name(worker_name)
            worker_id = w.id
        query = model_query(self.session, models.Task)
        query = query.options(
            joinedload(models.Task.policy),
            joinedload(models.Task.worker),
        )
        if 'deleted' not in kwargs:
            query = query.filter(models.Task.deleted == 'False')
        query = Database.sort(models.Task, query, sort_key, sort_dir)
        if worker_id >= 0:
            query = query.filter(models.Task.worker_id == worker_id)
        if task_name:
            query = query.filter(models.Task.name == task_name)
        total = query.count()
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query.all(), total

    def get_task(self, detail=True,  **kwargs):
        task_id = kwargs.get('id')
        query = model_query(self.session, models.Task)
        if detail:
            query = query.options(
                joinedload(models.Task.policy),
                joinedload(models.Task.worker),
            )
        query = query.filter(models.Task.id == task_id)
        task = query.first()
        if not task:
            raise NotFound()
        return task

    def get_task_by_id2(self,id2):
        query = model_query(self.session, models.Task)
        query = query.options(
            joinedload(models.Task.policy),
            joinedload(models.Task.worker),)
        query = query.filter(models.Task.id2 == id2)
        task = query.first()
        if not task:
            raise NotFound()
        return task


    def create_task(self, task_values):
        values = copy.deepcopy(task_values)
        values['id2'] = uuid.uuid1()
        task = models.Task()
        params = task.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        task.update(params)
        self.add(task)
        return self.get_task_by_id2(params['id2'])

    def update_task(self, task_values):
        values = copy.deepcopy(task_values)
        id = task_values.get('id')
        try:
            task = self.get_task(False, id=id)
        except:
            raise
        params = task.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        task.update(params)
        self.flush()
        return self.get_task(False, id=id)

    def delete_task(self, id):
        task = self.get_task(False, id=id)
        self.soft_delete(task)

    def get_policies(self, **kwargs):
        limit = kwargs.get('limit', 0)
        offset = kwargs.get('offset', 0)
        sort_key = kwargs.get('sort_key', 'created_at')
        sort_dir = kwargs.get('sort_dir', 'desc')
        query = model_query(self.session, models.Policy)
        query = Database.sort(models.Policy, query, sort_key, sort_dir)
        name = kwargs.get('name', 'unkown')
        if name != 'unkown':
            query = query.filter(models.Policy.name == name)
        if 'deleted' not in kwargs:
            query = query.filter(models.Policy.deleted == 'False')
        total = query.count()
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query.all(), total

    def get_policy(self, id, join=False, **kwargs):
        query = model_query(self.session, models.Policy)
        query = query.filter(models.Policy.id == id)
        if 'deleted' not in kwargs:
            query = query.filter(models.Policy.deleted == 'False')
        if join:
            query = query.options(
                joinedload(models.Policy.tasks)
            )
        policy = query.first()
        if not policy:
            raise NotFound()
        return policy

    def get_policy_by_name(self, name):
        query = model_query(self.session, models.Policy).\
            filter(models.Policy.name == name).\
            filter(models.Policy.deleted == 'False')

        policy = query.first()
        if not policy:
            raise NotFound()
        return policy

    def get_policy_id2(self, id2):
        query = model_query(self.session, models.Policy)
        query = query.filter(models.Policy.id2 == id2)
        return query.first()

    def create_policy(self, policy_values):
        print(policy_values)
        values = copy.deepcopy(policy_values)
        values['id2'] = uuid.uuid1()
        policy = models.Policy()
        params = policy.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        print(params)
        policy.update(params)
        self.add(policy)
        return self.get_policy_id2(values['id2'])

    def update_policy(self, policy_values):
        values = copy.deepcopy(policy_values)
        id = values['id']
        policy = self.get_policy(id)
        params = policy.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        policy.update(params)
        self.flush()
        return self.get_policy(id)

    def delete_policy(self, id):
        policy = self.get_policy(id, True)
        tasks = policy.tasks
        if not tasks:
            self.soft_delete(policy)
            return None
        else:
            return tasks

    def get_workers(self, **kwargs):
        limit = kwargs.get('limit', 0)
        offset = kwargs.get('offset', 0)
        sort_key = kwargs.get('sort_key', 'created_at')
        sort_dir = kwargs.get('sort_dir', 'desc')
        name = kwargs.get('name', 'unkown')
        query = model_query(self.session, models.Worker)
        query = Database.sort(models.Worker, query, sort_key, sort_dir)
        if 'deleted' not in kwargs:
            query = query.filter(models.Worker.deleted == 'False')
        total = query.count()
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        if name != 'unkown':
            query = query.filter(models.Worker.name == name)
        return query.all(), total

    def get_worker(self, id, join=False, **kwargs):
        query = model_query(self.session, models.Worker)
        query = query.filter(models.Worker.id == id)
        if 'deleted' not in kwargs:
            query = query.filter(models.Worker.deleted == 'False')
        if join:
            query = query.options(
                joinedload(models.Worker.tasks)
            )
        worker = query.first()
        if not worker:
            raise NotFound()
        return worker

    def get_worker_by_id2(self, id2):
        query = model_query(self.session, models.Worker)
        query = query.filter(models.Worker.id2 == id2)
        worker = query.first()
        if not worker:
            raise NotFound()
        return worker

    def get_worker_by_name(self, worker_name):
        worker = model_query(self.session, models.Worker).\
            filter(models.Worker.name == worker_name).first()
        return worker

    def create_worker(self, worker_values):
        values = copy.deepcopy(worker_values)
        values['id2'] = uuid.uuid1()
        worker = models.Worker()
        params = worker.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        worker.update(params)
        self.add(worker)
        return self.get_worker_by_id2(values['id2'])

    def update_worker(self, worker_values):
        values = copy.deepcopy(worker_values)
        id = values['id']
        try:
            worker = self.get_worker(id)
        except:
            raise
        params = worker.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        worker.update(params)
        self.flush()
        return self.get_worker(id)

    def delete_worker(self, id):
        worker = self.get_worker(id, True)
        tasks = worker.tasks
        if not tasks:
            self.soft_delete(worker)
            return None
        else:
            return tasks

    def get_users(self, **kwargs):
        limit = kwargs.get('limit', 0)
        offset = kwargs.get('offset', 0)
        sort_key = kwargs.get('sort_key', 'created_at')
        sort_dir = kwargs.get('sort_dir', 'desc')
        name = kwargs.get('name', 'unkown')
        query = model_query(self.session, models.User)
        query = Database.sort(models.Worker, query, sort_key, sort_dir)
        if 'deleted' not in kwargs:
            query = query.filter(models.User.deleted == 'False')
        total = query.count()
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        if name != 'unkown':
            query = query.filter(models.User.name == name)
        return query.all(),  total

    def get_user(self, id, join=False, **kwargs):
        query = model_query(self.session, models.User)
        query = query.filter(models.User.id == id)
        if 'deleted' not in kwargs:
            query = query.filter(models.User.deleted == 'False')
        if join:
            query = query.options(
                joinedload(models.User.tasks)
            )
        user = query.first()
        if not user:
            raise NotFound()
        return user

    def create_user(self, user_values):
        values = copy.deepcopy(user_values)
        values['id2'] = uuid.uuid1()
        user = models.User()
        params = user.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
            user.update(params)
        self.add(user)
        return self.get_model_by_id2(models.User, values['id2'])

    def update_user(self, user_values):
        values = copy.deepcopy(user_values)
        id = values['id']
        try:
            user = self.get_user(id)
        except:
            raise
        params = user.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
        user.update(params)
        self.flush()
        return self.get_user(id)

    def delete_user(self, id):
        user = self.get_user(id)
        tasks = user.tasks
        if not tasks:
            self.soft_delete(user)
            return None
        else:
            return tasks


    def bk_create(self, bk_values):
        values = copy.deepcopy(bk_values)
        values['id2'] = uuid.uuid1()
        state = models.BackupState()
        params = state.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
            state.update(params)
        self.add(state)
        return self.get_model_by_id2(models.BackupState, values['id2'])

    def get_bk_state(self, id, join=False, **kwargs):
        query = model_query(self.session, models.BackupState)
        query = query.filter(models.BackupState.id == id)
        if join:
            query = query.options(
                joinedload(models.BackupState.task)
            )
        if 'deleted' not in kwargs:
            query = query.filter(models.BackupState.deleted == 'False')
        state = query.first()
        if not state:
            raise NotFound()
        return state

    def bk_update(self, bk_values):
        values = copy.deepcopy(bk_values)
        id = values['id']
        try:
            state = self.get_bk_state(id)
        except:
            raise
        params = state.generate_param()
        for k, v in params.items():
            params[k] = values.get(k)
            state.update(params)
        self.flush()
        return self.get_bk_state(id)

    def get_task_by_name(self, name):
        query = model_query(self.session, models.Task).\
            filter(models.Task.name == name).\
            filter(models.Task.deleted == 'False')
        task = query.first()
        if not task:
            return NotFound()
        return task

    def bk_list(self, detail=False, **kwargs):
        limit = kwargs.get('limit', 0)
        offset = kwargs.get('offset', 0)
        sort_key = kwargs.get('sort_key', 'updated_at')
        sort_dir = kwargs.get('sort_dir', 'desc')
        task_name = kwargs.get('task_name')
        task_id = kwargs.get('task_id')
        query = model_query(self.session, models.BackupState)
        if detail:
            query.options(
                joinedload(models.BackupState.task)
            )
        if 'deleted' not in kwargs:
            query = query.filter(models.BackupState.deleted == 'False')
        if task_name:
            t = self.get_task_by_name(task_name)
            task_id = t.id
        if task_id:
            query = query.filter(models.BackupState.task_id == task_id)
        total = query.count()
        query = Database.sort(models.BackupState, query, sort_key, sort_dir)
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)
        return query.all(), total

def get_db(type, path):
    return Database(type, path)
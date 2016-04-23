# -*- coding: utf-8 -*-

from django.db import models

class Dummy(models.Model):
    
    uid = models.BigIntegerField(primary_key=True)
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True, db_index=True)
    
    @classmethod
    def get_partition_model(cls, create, *args, **kwargs):
        partition_key = ''
        if 'uid' in kwargs:
            partition_key = kwargs['uid']
        elif 'uuid' in kwargs:
            # get uid from uuid
            # Todo: get uid from redis, not from mysql
            try:
                extra = account_extra_uuid.objects.get(uuid=kwargs['uuid'])
                partition_key = extra.uid
            except account_extra_uuid.DoesNotExist:
                logger.warn("User extra uuid:%s not exsit." % kwargs['uuid'])
        return create_new_model(account, 'range', 'uid', partition_key, cls, create)

    @classmethod
    def get_partition(cls, *args, **kwargs):
        return account.get_partition_model(cls, False, *args, **kwargs)

    @staticmethod
    def __new__(cls, *args, **kwargs):
        return partition_model_new(account, cls, *args, **kwargs)
        
class account_extra_uuid(models.Model):
    uuid = models.UUIDField(primary_key=True)
    uid = models.BigIntegerField()        

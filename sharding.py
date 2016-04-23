# -*- coding: utf-8 -*-
from django.conf import settings
from django.db import connection
import logging
logger = logging.getLogger(__name__)


partition_proxy_key = "__partition__proxy__"


# partition algorithm
class PartitionSelector(object):
    """
    The partition selector class
    """
    def __init__(self, partition_type, source_table, column_name, column_value, create=False):
        """
        Args:
            partition_type: partition type (range, list, hash, key), nowadays support range and hash,
            source_table: new partition will created according to it,
            column_name: the partition column's name,
            column_value: the partition column's value,
            create: whether create the table as it not exsits.
        """
        self.partition_type = partition_type
        self.source_table = source_table
        self.column_name = column_name
        self.column_value = column_value
        self.create = create

    def __call__(self):
        rules = settings.PARTITION_RULES[self.partition_type]
        if self.partition_type == "range":
            if self.column_name in ["uid", "video_id"]:
                range_setting = rules[self.column_name]
                self.offset = range_setting['offset']
                self.max_per_table = range_setting['max_per_table']
                return self._partition_range()
        elif self.partition_type == "hash":
            if self.column_name == "live_id":
                hash_setting = rules[self.column_name]
                self.number = hash_setting['number']
                return self._partition_hash_live_id()

    def _partition_check(self, dest_table):
        """
        Check if the partition exists
        Creating operation is controlled by `self.create`
        """
        if not self.create:
            return dest_table
        if not self._partition_exists(dest_table):
            sql_create_table = "CREATE TABLE {0} LIKE {1};".format(
                                dest_table, self.source_table)
            cursor = connection.cursor()
            cursor.execute(sql_create_table)
            cursor.close()
            logger.info("[partition] Create db table : %s." % dest_table)
        return dest_table

    def _partition_exists(self, desc_table):
        """
        Checks if partition exists.
        Returns:
            1: exists
            2: not exists
        """
        logger.info("[partition] partition_exists db : %s." % settings.DATABASES["default"]["NAME"])
        sql = """
            select exists(
                select * from information_schema.tables where table_schema = '%s' and table_name = '%s');
        """ % (settings.DATABASES["default"]["NAME"], desc_table)
        cursor = connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0]

    def _partition_range(self):
        suffix = (self.column_value - self.offset) / self.max_per_table
        if suffix <= 0:
            return self.source_table
        dest_table = "%s_%s" % (self.source_table, suffix)
        return self._partition_check(dest_table)

    def _partition_hash_live_id(self):
        suffix = int(self.column_value) % int(self.number)
        if suffix <= 0:
            return self.source_table
        dest_table = "%s_%s" % (self.source_table, suffix)
        return self._partition_check(dest_table)


def clear_model_cache(_meta):
    from django.db.models.loading import cache
    try:
        del cache.all_models[_meta.app_label][_meta.model_name]
        logger.info('cache.app_models[%s][%s]' % (_meta.app_label,
                                            _meta.model_name))
    except KeyError:
        pass


def create_new_model(model, partition_type, partition_column, partition_key, cls, create=False):
    if partition_key:
        table_name = '%s_%s' % (cls._meta.app_label, cls._meta.model_name)
        partition = PartitionSelector(partition_type, table_name, partition_column, partition_key, create=create)()
        model_name = partition.replace('%s_' % cls._meta.app_label, '')
        clear_model_cache(cls._meta)
    else:
        model_name = cls._meta.model_name
        clear_model_cache(cls._meta)
    # structure new model
    setattr(model._meta, 'abstract', True)
    new_cls = type(model_name, (model, ), {'__module__': model.__module__})
    setattr(new_cls, 'DoesNotExist', model.DoesNotExist)
    return new_cls


def partition_model_new(model, cls, *args, **kwargs):
    if args:
        return super(model, cls).__new__(cls, *args, **kwargs)
    if partition_proxy_key in kwargs:
        del kwargs[partition_proxy_key]
        return super(model, cls).__new__(cls, *args, **kwargs)
    new_cls = model.get_partition_model(cls, True, *args, **kwargs)
    kwargs[partition_proxy_key] = True
    return new_cls.__new__(new_cls, *args, **kwargs)

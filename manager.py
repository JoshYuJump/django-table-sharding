class DummyManager():

    def get_queryset(self, *args, **kwargs):
        return super(MyUserManager, self).get_queryset(*args, **kwargs)

    def filter(self, *args, **kwargs):
        partition = self.model.get_partition(*args, **kwargs)
        return partition.objects.filter(*args, **kwargs)

    def get(self, *args, **kwargs):
        partition = self.model.get_partition(*args, **kwargs)
        return partition.objects.get(*args, **kwargs)

    def select_for_update(self, *args, **kwargs):
        partition = self.model.get_partition(*args, **kwargs)
        return partition.objects.select_for_update()

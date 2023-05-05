import asyncio
import logging

import aiomysql


logging.basicConfig(level=logging.INFO)


async def create_pool(loop,**kw):
    logging.info('create database connection poll...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port',3306),
        user=kw.get('user','root'),
        password=kw.get('password','123456'),
        db=kw.get('db','awesome'),
        charset=kw.get('charset','utf8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )


async def select(sql,args,size=None):
    logging.log(sql,args)
    global __pool
    with (await __pool) as conn:
        curse = await conn.cursor(aiomysql.DictCursor)
        await curse.execute(sql.replace('?','%s',args or ()))
        if size:
            rs = await curse.fetchmany(size)
        else:
            rs = await curse.fetchall()
        await curse.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


async def execute(sql,args):
    logging.info([sql,args])
    async with __pool.acquire() as conn:
        try:
            curse = await conn.cursor()
            await curse.execute(sql.replace('?','%s'), args)
            affected = curse.rowcount
            await curse.close()
        except BaseException as e:
            raise
        return affected


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # 排除Model类本身:
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取 table 名称
        tableName = attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的 Field 和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k,v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)

        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)

        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的 SELECT 、INSERT、UPDATE、DELETE语句
        attrs['__select__'] = 'select `%s` , %s from `%s`' % (primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"Model object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)


    def getValueOrDefault(self, key):
        value = getattr(self,key,None)

        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                # 如果field.default是可调用对象，则调用它并将结果赋给value；否则直接将field.default的值赋给value。
                # 这种写法通常用于为变量设置默认值时，可以通过定义一个可调用的函数或实例方法来动态生成默认值。
                value = field.default if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def find(self,cls,pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__,[pk],1))
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findAll(cls):



    async def save(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    # TODO findAll()、findNumber()、update()、remove()


class Field(object):
    def __init__(self,name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return "{field_type},{type}:{name}".format(field_type=self.__class__.__name__,type=self.column_type,name=self.name)


class StringField(Field):
    def __init__(self, name=None,primary_key=False,default=None,ddl='varchar(100)'):
        super().__init__(name,ddl,primary_key,default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0, ddl='bigint'):
        super().__init__(name, ddl, primary_key, default)

class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key=True)
    name = StringField()


async def test_save(loop):
    await create_pool(loop,user='root',password='123456',db='test')

    user = User( name='c')
    dir(user)
    await user.save()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_save(loop))
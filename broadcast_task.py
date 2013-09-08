from random import choice
from string import ascii_uppercase, digits
import boto.sdb
from flask import Flask
from celery import Celery
from subprocess import call

# vars:
static_dir = 'static/'
upload_dir = 'upload/'
domain_name = 'broadcast'
time_limit = '14'
choices = ascii_uppercase + digits

def make_celery(app):
    celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
    celery.conf.update(app.config)
    TaskBase = celery.Task
    class ContextTask(TaskBase):
        abstract = True
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery

# init:

conn = boto.sdb.connect_to_region(
    'us-east-1',
    aws_access_key_id='XXXXXXXXXX',
    aws_secret_access_key='XXXXXXXXXXXXXXXXXXXXXXXXXX')
try:
    dom = conn.get_domain(domain_name, validate=True)
except:
    print 'Making domain: '+domain_name+' for the first time\n'
    dom = conn.create_domain(domain_name)
print 'Connected to: '+str(dom)+'\n'
meta = conn.domain_metadata(dom)

app = Flask(__name__)
app.config.update(
    CELERY_BROKER_URL='amqp://guest:guest@localhost:5672//',
    CELERY_RESULT_BACKEND='amqp://guest:guest@localhost:5672//'
)
celery = make_celery(app)

# functions

def randstr(size=6):
    return ''.join(choice(choices) for x in xrange(size))

def place_new(val):
    if not val:
        print 'Nothing to do in place: empty value.. returning.\n'
        return
    if not hasattr(val, 'keys'):
        print 'Value passed is not dictionary-like: mapping to itself\n'
        val = {val:val}
    new_name = randstr()
    while dom.get_item(new_name):
        print 'Conflict on '+new_name+' -> '+str(dom.get_item(new_name))+')\n'
        new_name = randstr()
    dom.put_attributes(new_name, val)
    print 'Value placed at '+new_name+'.\n'
    print domain_name+' now has '+str(meta.item_count)+' entries.\n'
    return new_name

@celery.task()
def add_file(fname, email, photo=None):
    new_name = place_new({'filename':''})
    nfname = new_name + '.mp3'
    
    call(['ffmpeg','-t',time_limit,'-i',upload_dir+fname,static_dir+nfname])
    
    item = dom.get_item(new_name)
    item['filename'] = nfname
    item.save()

    # TODO send email w/link
    
    # TODO save file  to s3 instead of static_dir : http://boto.readthedocs.org/en/latest/s3_tut.html
    
    # TODO add delete link and photo
    

    return


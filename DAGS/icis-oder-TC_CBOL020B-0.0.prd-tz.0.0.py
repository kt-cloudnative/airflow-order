from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='TC_CBOL020B',WORKFLOW_ID='6bcae78f32254ffdaf6423322970c17a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-TC_CBOL020B-0.0.prd-tz.0.0'
    ,'schedule_interval':'30 0 * * *'
    ,'start_date': datetime(2024, 10, 29, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('6bcae78f32254ffdaf6423322970c17a')

    cbol032bJob_vol = []
    cbol032bJob_volMnt = []
    cbol032bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol032bJob_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    cbol032bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol032bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'adb07b7b130a4668915b1c5a183cf616',
        'volumes': cbol032bJob_vol,
        'volume_mounts': cbol032bJob_volMnt,
        'env_from':cbol032bJob_env,
        'task_id':'cbol032bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.5',
        'arguments':["--job.name=cbol032bJob", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbol034bJob_vol = []
    cbol034bJob_volMnt = []
    cbol034bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol034bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol034bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol034bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1c546db5a1bb4d53b31bb61262962b66',
        'volumes': cbol034bJob_vol,
        'volume_mounts': cbol034bJob_volMnt,
        'env_from':cbol034bJob_env,
        'task_id':'cbol034bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol034bJob", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbol033bJob_vol = []
    cbol033bJob_volMnt = []
    cbol033bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol033bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol033bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol033bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4a2e308bb1ed4ca9b07196e0fd8ea0f8',
        'volumes': cbol033bJob_vol,
        'volume_mounts': cbol033bJob_volMnt,
        'env_from':cbol033bJob_env,
        'task_id':'cbol033bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol033bJob", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbol020bJob_vol = []
    cbol020bJob_volMnt = []
    cbol020bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol020bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol020bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol020bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '539399a1cccc41c297304dca60a2c4ae',
        'volumes': cbol020bJob_vol,
        'volume_mounts': cbol020bJob_volMnt,
        'env_from':cbol020bJob_env,
        'task_id':'cbol020bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol020bJob", "gubun=ALL", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbol035bJob_vol = []
    cbol035bJob_volMnt = []
    cbol035bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol035bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol035bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol035bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a6975f58001544679b4ce6a723d710a5',
        'volumes': cbol035bJob_vol,
        'volume_mounts': cbol035bJob_volMnt,
        'env_from':cbol035bJob_env,
        'task_id':'cbol035bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol035bJob", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbol024bJob_vol = []
    cbol024bJob_volMnt = []
    cbol024bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol024bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol024bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol024bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2361b304e54c49eda2e940f59387a0d6',
        'volumes': cbol024bJob_vol,
        'volume_mounts': cbol024bJob_volMnt,
        'env_from':cbol024bJob_env,
        'task_id':'cbol024bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol024bJob", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbol022bJob_vol = []
    cbol022bJob_volMnt = []
    cbol022bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol022bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol022bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol022bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '12bcc0591bd946a8b47978defecb6e0b',
        'volumes': cbol022bJob_vol,
        'volume_mounts': cbol022bJob_volMnt,
        'env_from':cbol022bJob_env,
        'task_id':'cbol022bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol022bJob2", "requestDate=${YYYYMMDD}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('6bcae78f32254ffdaf6423322970c17a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol020bJob,
        cbol022bJob,
        cbol024bJob,
        cbol032bJob,
        cbol033bJob,
        cbol034bJob,
        cbol035bJob,
        Complete
    ]) 

    # authCheck >> cbol020bJob >> cbol022bJob >> cbol024bJob >> cbol032bJob >> cbol033bJob >> cbol034bJob >> cbol035bJob >> Complete
    workflow









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
                , WORKFLOW_NAME='WC_CSNG504B',WORKFLOW_ID='c7c074f1c20743248027289e76f9f304', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CSNG504B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c7c074f1c20743248027289e76f9f304')

    csng504bJob_0131_vol = []
    csng504bJob_0131_volMnt = []
    csng504bJob_0131_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0131_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0131_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0131_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0131_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0131 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '843d681121fb477dbf9ef88ccd457bdf',
        'volumes': csng504bJob_0131_vol,
        'volume_mounts': csng504bJob_0131_volMnt,
        'env_from':csng504bJob_0131_env,
        'task_id':'csng504bJob_0131',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0131"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0502_vol = []
    csng504bJob_0502_volMnt = []
    csng504bJob_0502_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0502_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0502_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0502_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0502_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0502 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f4b8fdf2173a4669a1c378a76b7ba938',
        'volumes': csng504bJob_0502_vol,
        'volume_mounts': csng504bJob_0502_volMnt,
        'env_from':csng504bJob_0502_env,
        'task_id':'csng504bJob_0502',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0502"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0060_vol = []
    csng504bJob_0060_volMnt = []
    csng504bJob_0060_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0060_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0060_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0060_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0060_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0060 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ce3c889054804daabb20d8f7ad99cb9f',
        'volumes': csng504bJob_0060_vol,
        'volume_mounts': csng504bJob_0060_volMnt,
        'env_from':csng504bJob_0060_env,
        'task_id':'csng504bJob_0060',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0060"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0303_vol = []
    csng504bJob_0303_volMnt = []
    csng504bJob_0303_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0303_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0303_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0303_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0303_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0303 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2c6d8251902048f0a134e56956d0238d',
        'volumes': csng504bJob_0303_vol,
        'volume_mounts': csng504bJob_0303_volMnt,
        'env_from':csng504bJob_0303_env,
        'task_id':'csng504bJob_0303',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0303"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng504bJob_0080_vol = []
    csng504bJob_0080_volMnt = []
    csng504bJob_0080_vol.append(getVolume('shared-volume','shared-volume'))
    csng504bJob_0080_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng504bJob_0080_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng504bJob_0080_env.extend([getICISConfigMap('icis-oder-wrlincomn-batch-mng-configmap'), getICISSecret('icis-oder-wrlincomn-batch-mng-secret'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISSecret('icis-oder-wrlincomn-batch-secret')])
    csng504bJob_0080_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng504bJob_0080 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9baaf479ab044627a7ff514d2f8a249f',
        'volumes': csng504bJob_0080_vol,
        'volume_mounts': csng504bJob_0080_volMnt,
        'env_from':csng504bJob_0080_env,
        'task_id':'csng504bJob_0080',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.7',
        'arguments':["--job.name=csng504bJob", "date="+str(datetime.now()), "inputDate=${YYYYMM}01", "workMode=0080"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c7c074f1c20743248027289e76f9f304')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng504bJob_0502,
        csng504bJob_0060,
        csng504bJob_0303,
        csng504bJob_0080,
        csng504bJob_0131,
        Complete
    ]) 

    # authCheck >> csng504bJob_0502 >> csng504bJob_0060 >> csng504bJob_0303>> csng504bJob_0080 >> csng504bJob_0131 >> Complete
    workflow









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
                , WORKFLOW_NAME='PP_CSNC988B',WORKFLOW_ID='8a5180172229480e8a45c1582c8b9b10', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC988B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8a5180172229480e8a45c1582c8b9b10')

    csnc988bJob_0_vol = []
    csnc988bJob_0_volMnt = []
    csnc988bJob_0_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_0_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_0_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_0_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_0_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_0 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '54dc341ef3104ab29ac3198dafeddac2',
        'volumes': csnc988bJob_0_vol,
        'volume_mounts': csnc988bJob_0_volMnt,
        'env_from':csnc988bJob_0_env,
        'task_id':'csnc988bJob_0',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=0", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_1_vol = []
    csnc988bJob_1_volMnt = []
    csnc988bJob_1_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_1_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_1_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_1_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_1_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_1 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b21d5332528e49d79812f89a868cc9ac',
        'volumes': csnc988bJob_1_vol,
        'volume_mounts': csnc988bJob_1_volMnt,
        'env_from':csnc988bJob_1_env,
        'task_id':'csnc988bJob_1',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=1", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_2_vol = []
    csnc988bJob_2_volMnt = []
    csnc988bJob_2_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_2_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_2_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_2_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_2_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_2 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'aeee81f8198e46f2b3bec08d8bdf1aee',
        'volumes': csnc988bJob_2_vol,
        'volume_mounts': csnc988bJob_2_volMnt,
        'env_from':csnc988bJob_2_env,
        'task_id':'csnc988bJob_2',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=2", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_3_vol = []
    csnc988bJob_3_volMnt = []
    csnc988bJob_3_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_3_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_3_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_3_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_3_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_3 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'defb2a07984d4c85a513e17d09bae07f',
        'volumes': csnc988bJob_3_vol,
        'volume_mounts': csnc988bJob_3_volMnt,
        'env_from':csnc988bJob_3_env,
        'task_id':'csnc988bJob_3',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=3", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_4_vol = []
    csnc988bJob_4_volMnt = []
    csnc988bJob_4_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_4_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_4_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_4_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_4_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_4 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '57dd5fd8197a49e4b121a8a3c1ca3de7',
        'volumes': csnc988bJob_4_vol,
        'volume_mounts': csnc988bJob_4_volMnt,
        'env_from':csnc988bJob_4_env,
        'task_id':'csnc988bJob_4',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=4", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_5_vol = []
    csnc988bJob_5_volMnt = []
    csnc988bJob_5_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_5_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_5_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_5_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_5_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_5 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '99e4cdf1284c490083b82bf09c45af36',
        'volumes': csnc988bJob_5_vol,
        'volume_mounts': csnc988bJob_5_volMnt,
        'env_from':csnc988bJob_5_env,
        'task_id':'csnc988bJob_5',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=5", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_6_vol = []
    csnc988bJob_6_volMnt = []
    csnc988bJob_6_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_6_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_6_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_6_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_6_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_6 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '35f2e80ecb1e4f25aac69fe862c7d3ed',
        'volumes': csnc988bJob_6_vol,
        'volume_mounts': csnc988bJob_6_volMnt,
        'env_from':csnc988bJob_6_env,
        'task_id':'csnc988bJob_6',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=6", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_7_vol = []
    csnc988bJob_7_volMnt = []
    csnc988bJob_7_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_7_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_7_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_7_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_7_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_7 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0e6dd969c41e4cfdb2aa1fb1a7a01f49',
        'volumes': csnc988bJob_7_vol,
        'volume_mounts': csnc988bJob_7_volMnt,
        'env_from':csnc988bJob_7_env,
        'task_id':'csnc988bJob_7',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=7", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_8_vol = []
    csnc988bJob_8_volMnt = []
    csnc988bJob_8_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_8_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_8_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_8_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_8_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_8 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '916f4571c8144646afa03a0639c3c8f4',
        'volumes': csnc988bJob_8_vol,
        'volume_mounts': csnc988bJob_8_volMnt,
        'env_from':csnc988bJob_8_env,
        'task_id':'csnc988bJob_8',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=8", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csnc988bJob_9_vol = []
    csnc988bJob_9_volMnt = []
    csnc988bJob_9_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_9_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_9_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_9_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_9_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_9 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4210c76375a44808bee05f30504e06a2',
        'volumes': csnc988bJob_9_vol,
        'volume_mounts': csnc988bJob_9_volMnt,
        'env_from':csnc988bJob_9_env,
        'task_id':'csnc988bJob_9',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.5',
        'arguments':["--job.name=csnc988bJob", "subSaId=9", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8a5180172229480e8a45c1582c8b9b10')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnc988bJob_0,
        csnc988bJob_1,
        csnc988bJob_2,
        csnc988bJob_3,
        csnc988bJob_4,
        csnc988bJob_5,
        csnc988bJob_6,
        csnc988bJob_7,
        csnc988bJob_8,
        csnc988bJob_9,
        Complete
    ]) 

    # authCheck >> csnc988bJob_0 >> csnc988bJob_1 >> csnc988bJob_2 >> csnc988bJob_3 >> csnc988bJob_4 >> csnc988bJob_5 >> csnc988bJob_6 >> csnc988bJob_7 >> csnc988bJob_8 >> csnc988bJob_9 >> Complete
    workflow









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
                , WORKFLOW_NAME='PP_CSNC988B',WORKFLOW_ID='8937e68d9c6c45aa85ecb825ec237f87', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNC988B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8937e68d9c6c45aa85ecb825ec237f87')

    csnc988bJob_0_vol = []
    csnc988bJob_0_volMnt = []
    csnc988bJob_0_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnc988bJob_0_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnc988bJob_0_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnc988bJob_0_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csnc988bJob_0_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnc988bJob_0 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '7111ff58266445ebbb94d08f18705759',
        'volumes': csnc988bJob_0_vol,
        'volume_mounts': csnc988bJob_0_volMnt,
        'env_from':csnc988bJob_0_env,
        'task_id':'csnc988bJob_0',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'd9c51748373d468da17cc613d5d6eee2',
        'volumes': csnc988bJob_1_vol,
        'volume_mounts': csnc988bJob_1_volMnt,
        'env_from':csnc988bJob_1_env,
        'task_id':'csnc988bJob_1',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'd14c189dc619483285cd26f7d08118a0',
        'volumes': csnc988bJob_2_vol,
        'volume_mounts': csnc988bJob_2_volMnt,
        'env_from':csnc988bJob_2_env,
        'task_id':'csnc988bJob_2',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : '9cbe80dd65ed4419b747d8abc235d592',
        'volumes': csnc988bJob_3_vol,
        'volume_mounts': csnc988bJob_3_volMnt,
        'env_from':csnc988bJob_3_env,
        'task_id':'csnc988bJob_3',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'e6f5f3eeb3ce43b984a03f277008b7ce',
        'volumes': csnc988bJob_4_vol,
        'volume_mounts': csnc988bJob_4_volMnt,
        'env_from':csnc988bJob_4_env,
        'task_id':'csnc988bJob_4',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'c80322ee2a124c93891eb2e737c34a24',
        'volumes': csnc988bJob_5_vol,
        'volume_mounts': csnc988bJob_5_volMnt,
        'env_from':csnc988bJob_5_env,
        'task_id':'csnc988bJob_5',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'd536fed7814c442f84a75d4d7c90238b',
        'volumes': csnc988bJob_6_vol,
        'volume_mounts': csnc988bJob_6_volMnt,
        'env_from':csnc988bJob_6_env,
        'task_id':'csnc988bJob_6',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : '0bea5734850d4b53ac535de5b4c0fe0b',
        'volumes': csnc988bJob_7_vol,
        'volume_mounts': csnc988bJob_7_volMnt,
        'env_from':csnc988bJob_7_env,
        'task_id':'csnc988bJob_7',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'a5f0126c492d47d099fd1b423e8bcf4b',
        'volumes': csnc988bJob_8_vol,
        'volume_mounts': csnc988bJob_8_volMnt,
        'env_from':csnc988bJob_8_env,
        'task_id':'csnc988bJob_8',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
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
        'id' : 'ee606541a6114d8f88309bbf46e2a9d0',
        'volumes': csnc988bJob_9_vol,
        'volume_mounts': csnc988bJob_9_volMnt,
        'env_from':csnc988bJob_9_env,
        'task_id':'csnc988bJob_9',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csnc988bJob", "subSaId=9", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8937e68d9c6c45aa85ecb825ec237f87')

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









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
                , WORKFLOW_NAME='PP_CSNG923B',WORKFLOW_ID='bdb6de20afef4037b7c33af67de51b1c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG923B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bdb6de20afef4037b7c33af67de51b1c')

    csng923bJob_0_vol = []
    csng923bJob_0_volMnt = []
    csng923bJob_0_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_0_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_0_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_0_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_0_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_0 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2ce9e97777504b4c8e0a10fff905f135',
        'volumes': csng923bJob_0_vol,
        'volume_mounts': csng923bJob_0_volMnt,
        'env_from':csng923bJob_0_env,
        'task_id':'csng923bJob_0',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=0",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_1_vol = []
    csng923bJob_1_volMnt = []
    csng923bJob_1_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_1_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_1_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_1_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_1_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_1 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0dc1c86afcb3420db4c6e4553fe181be',
        'volumes': csng923bJob_1_vol,
        'volume_mounts': csng923bJob_1_volMnt,
        'env_from':csng923bJob_1_env,
        'task_id':'csng923bJob_1',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=1",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_2_vol = []
    csng923bJob_2_volMnt = []
    csng923bJob_2_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_2_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_2_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_2_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_2_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_2 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '0ea818261eb84feebd48e0fe2157b6c2',
        'volumes': csng923bJob_2_vol,
        'volume_mounts': csng923bJob_2_volMnt,
        'env_from':csng923bJob_2_env,
        'task_id':'csng923bJob_2',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=2",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_3_vol = []
    csng923bJob_3_volMnt = []
    csng923bJob_3_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_3_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_3_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_3_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_3_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_3 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '676502af4a8e480fbfc0a62c0ffd5608',
        'volumes': csng923bJob_3_vol,
        'volume_mounts': csng923bJob_3_volMnt,
        'env_from':csng923bJob_3_env,
        'task_id':'csng923bJob_3',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=3",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_4_vol = []
    csng923bJob_4_volMnt = []
    csng923bJob_4_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_4_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_4_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_4_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_4_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_4 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6f6d4dd8bb9740ebb04d18378c682b6a',
        'volumes': csng923bJob_4_vol,
        'volume_mounts': csng923bJob_4_volMnt,
        'env_from':csng923bJob_4_env,
        'task_id':'csng923bJob_4',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=4",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_5_vol = []
    csng923bJob_5_volMnt = []
    csng923bJob_5_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_5_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_5_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_5_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_5_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_5 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1d58c6e4bf3e4b558194583d2d16f060',
        'volumes': csng923bJob_5_vol,
        'volume_mounts': csng923bJob_5_volMnt,
        'env_from':csng923bJob_5_env,
        'task_id':'csng923bJob_5',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=5",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_6_vol = []
    csng923bJob_6_volMnt = []
    csng923bJob_6_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_6_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_6_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_6_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_6_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_6 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cdc0e934081b41ed92862832330ce863',
        'volumes': csng923bJob_6_vol,
        'volume_mounts': csng923bJob_6_volMnt,
        'env_from':csng923bJob_6_env,
        'task_id':'csng923bJob_6',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=6",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_7_vol = []
    csng923bJob_7_volMnt = []
    csng923bJob_7_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_7_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_7_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_7_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_7_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_7 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '54aa51e8f0394d58ad50ec2893b34f18',
        'volumes': csng923bJob_7_vol,
        'volume_mounts': csng923bJob_7_volMnt,
        'env_from':csng923bJob_7_env,
        'task_id':'csng923bJob_7',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=7",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_8_vol = []
    csng923bJob_8_volMnt = []
    csng923bJob_8_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_8_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_8_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_8_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_8_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_8 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c695edbef7c94846919b6281da1b2866',
        'volumes': csng923bJob_8_vol,
        'volume_mounts': csng923bJob_8_volMnt,
        'env_from':csng923bJob_8_env,
        'task_id':'csng923bJob_8',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=8",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    csng923bJob_9_vol = []
    csng923bJob_9_volMnt = []
    csng923bJob_9_vol.append(getVolume('shared-volume','shared-volume'))
    csng923bJob_9_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng923bJob_9_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng923bJob_9_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng923bJob_9_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng923bJob_9 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e9b35a7a1d394e1d96a9ee3a30a0e70a',
        'volumes': csng923bJob_9_vol,
        'volume_mounts': csng923bJob_9_volMnt,
        'env_from':csng923bJob_9_env,
        'task_id':'csng923bJob_9',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng923bJob", 
"inputDate=20241129",
"subStrSaId=9",
"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bdb6de20afef4037b7c33af67de51b1c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng923bJob_0,
        csng923bJob_1,
        csng923bJob_2,
        csng923bJob_3,
        csng923bJob_4,
        csng923bJob_5,
        csng923bJob_6,
        csng923bJob_7,
        csng923bJob_8,
        csng923bJob_9,
        Complete
    ]) 

    # authCheck >> csng923bJob_0 >> csng923bJob_1 >> csng923bJob_2 >> csng923bJob_3 >> csng923bJob_4 >> csng923bJob_5 >> csng923bJob_6 >> csng923bJob_7 >> csng923bJob_8 >> csng923bJob_9 >> Complete
    workflow









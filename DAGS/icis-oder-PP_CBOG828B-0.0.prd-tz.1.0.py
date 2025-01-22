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
                , WORKFLOW_NAME='PP_CBOG828B',WORKFLOW_ID='0beef54e886a43949b499bf0795d4c18', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG828B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 35, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('0beef54e886a43949b499bf0795d4c18')

    cbog828bJob_vol = []
    cbog828bJob_volMnt = []
    cbog828bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog828bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog828bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog828bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog828bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog828bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'afd2d26c5ee04872b1bee6f51fb96e37',
        'volumes': cbog828bJob_vol,
        'volume_mounts': cbog828bJob_volMnt,
        'env_from':cbog828bJob_env,
        'task_id':'cbog828bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog828bJob", "month=202411, "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_1_vol = []
    cbog829bJob_1_volMnt = []
    cbog829bJob_1_vol.append(getVolume('shared-volume','shared-volume'))
    cbog829bJob_1_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog829bJob_1_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_1_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_1_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_1 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '732b5468125041848dee2fdd91bd92d0',
        'volumes': cbog829bJob_1_vol,
        'volume_mounts': cbog829bJob_1_volMnt,
        'env_from':cbog829bJob_1_env,
        'task_id':'cbog829bJob_1',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=1","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_2_vol = []
    cbog829bJob_2_volMnt = []
    cbog829bJob_2_vol.append(getVolume('shared-volume','shared-volume'))
    cbog829bJob_2_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog829bJob_2_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_2_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_2_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_2 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a4e0f1eb67704cc9af4a5e3fbb84b379',
        'volumes': cbog829bJob_2_vol,
        'volume_mounts': cbog829bJob_2_volMnt,
        'env_from':cbog829bJob_2_env,
        'task_id':'cbog829bJob_2',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=2","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_3_vol = []
    cbog829bJob_3_volMnt = []
    cbog829bJob_3_vol.append(getVolume('shared-volume','shared-volume'))
    cbog829bJob_3_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog829bJob_3_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_3_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_3_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_3 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '232e1defecc44e16933372c9fd5e840a',
        'volumes': cbog829bJob_3_vol,
        'volume_mounts': cbog829bJob_3_volMnt,
        'env_from':cbog829bJob_3_env,
        'task_id':'cbog829bJob_3',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=3","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_4_vol = []
    cbog829bJob_4_volMnt = []
    cbog829bJob_4_vol.append(getVolume('shared-volume','shared-volume'))
    cbog829bJob_4_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog829bJob_4_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_4_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_4_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_4 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5e5547c6ce2946fb8a3aa28fb8537052',
        'volumes': cbog829bJob_4_vol,
        'volume_mounts': cbog829bJob_4_volMnt,
        'env_from':cbog829bJob_4_env,
        'task_id':'cbog829bJob_4',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=4","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_5_vol = []
    cbog829bJob_5_volMnt = []
    cbog829bJob_5_vol.append(getVolume('shared-volume','shared-volume'))
    cbog829bJob_5_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog829bJob_5_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_5_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_5_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_5 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'aafb4365471d45d3aeb7fb7843bfd34e',
        'volumes': cbog829bJob_5_vol,
        'volume_mounts': cbog829bJob_5_volMnt,
        'env_from':cbog829bJob_5_env,
        'task_id':'cbog829bJob_5',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=5","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_6_vol = []
    cbog829bJob_6_volMnt = []
    cbog829bJob_6_vol.append(getVolume('shared-volume','shared-volume'))
    cbog829bJob_6_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog829bJob_6_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_6_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_6_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_6 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'af471e530b9a44189ed7d0013ae0b7ab',
        'volumes': cbog829bJob_6_vol,
        'volume_mounts': cbog829bJob_6_volMnt,
        'env_from':cbog829bJob_6_env,
        'task_id':'cbog829bJob_6',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog829bJob" , "procNo=5","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('0beef54e886a43949b499bf0795d4c18')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog828bJob,
        cbog829bJob_1,
        cbog829bJob_2,
        cbog829bJob_3,
        cbog829bJob_4,
        cbog829bJob_5,
        cbog829bJob_6,
        Complete
    ]) 

    # authCheck >> cbog828bJob >> cbog829bJob_1 >> cbog829bJob_2 >> cbog829bJob_3 >> cbog829bJob_4 >> cbog829bJob_5 >> cbog829bJob_6 >> Complete
    workflow









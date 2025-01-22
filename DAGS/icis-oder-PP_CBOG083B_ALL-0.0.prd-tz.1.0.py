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
                , WORKFLOW_NAME='PP_CBOG083B_ALL',WORKFLOW_ID='f7f5493814bc4ca48242169e563c296c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG083B_ALL-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f7f5493814bc4ca48242169e563c296c')

    PP_CBOG083B_PS_vol = []
    PP_CBOG083B_PS_volMnt = []
    PP_CBOG083B_PS_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_PS_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_PS_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_PS_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_PS_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_PS = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '8c850a5c7ab94266b5e6abfb782addbe',
        'volumes': PP_CBOG083B_PS_vol,
        'volume_mounts': PP_CBOG083B_PS_volMnt,
        'env_from':PP_CBOG083B_PS_env,
        'task_id':'PP_CBOG083B_PS',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=PS", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_JB_vol = []
    PP_CBOG083B_JB_volMnt = []
    PP_CBOG083B_JB_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_JB_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_JB_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_JB_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_JB_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_JB = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a695a77cc90c46189e046bca5316c663',
        'volumes': PP_CBOG083B_JB_vol,
        'volume_mounts': PP_CBOG083B_JB_volMnt,
        'env_from':PP_CBOG083B_JB_env,
        'task_id':'PP_CBOG083B_JB',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=JB", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_KW_vol = []
    PP_CBOG083B_KW_volMnt = []
    PP_CBOG083B_KW_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_KW_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_KW_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_KW_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_KW_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_KW = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '843a5a417e56406abf398ae2101e2a8d',
        'volumes': PP_CBOG083B_KW_vol,
        'volume_mounts': PP_CBOG083B_KW_volMnt,
        'env_from':PP_CBOG083B_KW_env,
        'task_id':'PP_CBOG083B_KW',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=KW", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_CN_vol = []
    PP_CBOG083B_CN_volMnt = []
    PP_CBOG083B_CN_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_CN_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_CN_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_CN_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_CN_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_CN = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd95e7028a13343c0809c6b07cfd652ee',
        'volumes': PP_CBOG083B_CN_vol,
        'volume_mounts': PP_CBOG083B_CN_volMnt,
        'env_from':PP_CBOG083B_CN_env,
        'task_id':'PP_CBOG083B_CN',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=CN", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_KK_vol = []
    PP_CBOG083B_KK_volMnt = []
    PP_CBOG083B_KK_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_KK_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_KK_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_KK_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_KK_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_KK = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bff50588a4f64ee8a979f0b1a6ea4cdb',
        'volumes': PP_CBOG083B_KK_vol,
        'volume_mounts': PP_CBOG083B_KK_volMnt,
        'env_from':PP_CBOG083B_KK_env,
        'task_id':'PP_CBOG083B_KK',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=KK", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_DK_vol = []
    PP_CBOG083B_DK_volMnt = []
    PP_CBOG083B_DK_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_DK_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_DK_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_DK_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_DK_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_DK = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6e496df4d84f4c9a95539e690069f6fb',
        'volumes': PP_CBOG083B_DK_vol,
        'volume_mounts': PP_CBOG083B_DK_volMnt,
        'env_from':PP_CBOG083B_DK_env,
        'task_id':'PP_CBOG083B_DK',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=DK", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_JN_vol = []
    PP_CBOG083B_JN_volMnt = []
    PP_CBOG083B_JN_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_JN_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_JN_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_JN_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_JN_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_JN = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bef7166175524e47a37818186baaed51',
        'volumes': PP_CBOG083B_JN_vol,
        'volume_mounts': PP_CBOG083B_JN_volMnt,
        'env_from':PP_CBOG083B_JN_env,
        'task_id':'PP_CBOG083B_JN',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=JN", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_JJ_vol = []
    PP_CBOG083B_JJ_volMnt = []
    PP_CBOG083B_JJ_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_JJ_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_JJ_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_JJ_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_JJ_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_JJ = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'fefa19611c0c40ea980f4c4034a0d09e',
        'volumes': PP_CBOG083B_JJ_vol,
        'volume_mounts': PP_CBOG083B_JJ_volMnt,
        'env_from':PP_CBOG083B_JJ_env,
        'task_id':'PP_CBOG083B_JJ',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=JJ", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    PP_CBOG083B_SL_vol = []
    PP_CBOG083B_SL_volMnt = []
    PP_CBOG083B_SL_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    PP_CBOG083B_SL_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    PP_CBOG083B_SL_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    PP_CBOG083B_SL_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    PP_CBOG083B_SL_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    PP_CBOG083B_SL = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '870ff379519d49f6938838bd690822c5',
        'volumes': PP_CBOG083B_SL_vol,
        'volume_mounts': PP_CBOG083B_SL_volMnt,
        'env_from':PP_CBOG083B_SL_env,
        'task_id':'PP_CBOG083B_SL',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog083bJob", "rsName=SL", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f7f5493814bc4ca48242169e563c296c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        PP_CBOG083B_SL,
        PP_CBOG083B_PS,
        PP_CBOG083B_KK,
        PP_CBOG083B_JN,
        PP_CBOG083B_DK,
        PP_CBOG083B_CN,
        PP_CBOG083B_JB,
        PP_CBOG083B_KW,
        PP_CBOG083B_JJ,
        Complete
    ]) 

    # authCheck >> PP_CBOG083B_SL >> PP_CBOG083B_PS >> PP_CBOG083B_KK >> PP_CBOG083B_JN >> PP_CBOG083B_DK >> PP_CBOG083B_CN >> PP_CBOG083B_JB >> PP_CBOG083B_KW >> PP_CBOG083B_JJ >> Complete
    workflow









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
                , WORKFLOW_NAME='tkdgns1022',WORKFLOW_ID='4a99f1d499784d369551770a5e45cff5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-tkdgns1022-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 25, 5, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('4a99f1d499784d369551770a5e45cff5')

    tkdgns_vol = []
    tkdgns_volMnt = []
    tkdgns_vol.append(getVolume('shared-volume','shared-volume'))
    tkdgns_volMnt.append(getVolumeMount('shared-volume','tkdgns'))

    tkdgns_vol.append(getVolume('tkdgns','tkdgns'))
    tkdgns_volMnt.append(getVolumeMount('tkdgns','/etc/'))

    tkdgns_env = [getICISConfigMap('icis-oder-pvctest-configmap'), getICISConfigMap('icis-oder-pvctest-configmap2'), getICISSecret('icis-oder-pvctest-secret')]
    tkdgns_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    tkdgns_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    tkdgns = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bec234d43be74671ac6c724591fdbf6e',
        'volumes': tkdgns_vol,
        'volume_mounts': tkdgns_volMnt,
        'env_from':tkdgns_env,
        'task_id':'tkdgns',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    tkdtkdgns_vol = []
    tkdtkdgns_volMnt = []
    tkdtkdgns_vol.append(getVolume('33333','33333'))
    tkdtkdgns_volMnt.append(getVolumeMount('33333','33333'))

    tkdtkdgns_env = [getICISConfigMap('icis-oder-pvctest-configmap'), getICISConfigMap('icis-oder-pvctest-configmap2'), getICISSecret('icis-oder-pvctest-secret')]
    tkdtkdgns_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    tkdtkdgns_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    tkdtkdgns = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '198e79a04a054e37bc492d00b3405580',
        'volumes': tkdtkdgns_vol,
        'volume_mounts': tkdtkdgns_volMnt,
        'env_from':tkdtkdgns_env,
        'task_id':'tkdtkdgns',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    gnstkd_vol = []
    gnstkd_volMnt = []
    gnstkd_vol.append(getVolume('shared-volume','shared-volume'))
    gnstkd_volMnt.append(getVolumeMount('shared-volume','222222'))

    gnstkd_vol.append(getVolume('d1023','d1023'))
    gnstkd_volMnt.append(getVolumeMount('d1023','1308'))

    gnstkd_env = [getICISConfigMap('icis-oder-pvctest-configmap'), getICISConfigMap('icis-oder-pvctest-configmap2'), getICISSecret('icis-oder-pvctest-secret')]
    gnstkd_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    gnstkd_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    gnstkd = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '66e08d5543b3453a9a7d64ef1cc2703c',
        'volumes': gnstkd_vol,
        'volume_mounts': gnstkd_volMnt,
        'env_from':gnstkd_env,
        'task_id':'gnstkd',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('4a99f1d499784d369551770a5e45cff5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        tkdgns,
        gnstkd,
        tkdtkdgns,
        Complete
    ]) 

    # authCheck >> tkdgns >> gnstkd >> tkdtkdgns >> Complete
    workflow









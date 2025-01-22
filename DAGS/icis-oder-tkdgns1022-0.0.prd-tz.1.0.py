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
                , WORKFLOW_NAME='tkdgns1022',WORKFLOW_ID='3b5e7d717edd41cea11aaa20a67c4f5c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-tkdgns1022-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 10, 25, 5, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3b5e7d717edd41cea11aaa20a67c4f5c')

    gnstkd_vol = []
    gnstkd_volMnt = []
    gnstkd_vol.append(getVolume('shared-volume','shared-volume'))
    gnstkd_volMnt.append(getVolumeMount('shared-volume','222222'))

    gnstkd_vol.append(getVolume('10221620','10221620'))
    gnstkd_volMnt.append(getVolumeMount('10221620','tkdgns'))

    gnstkd_env = [getICISConfigMap('icis-oder-pvctest-configmap'), getICISConfigMap('icis-oder-pvctest-configmap2'), getICISSecret('icis-oder-pvctest-secret')]
    gnstkd_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    gnstkd_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    gnstkd = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '46302603706345358f484bc771a2054a',
        'volumes': gnstkd_vol,
        'volume_mounts': gnstkd_volMnt,
        'env_from':gnstkd_env,
        'task_id':'gnstkd',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

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
        'id' : '581cdc6d917d46a1b4a4a58346b2fef7',
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
        'id' : 'ad9a6d06e86648bbaae9c79b04ce9f2f',
        'volumes': tkdtkdgns_vol,
        'volume_mounts': tkdtkdgns_volMnt,
        'env_from':tkdtkdgns_env,
        'task_id':'tkdtkdgns',
        'image':'/icis/icis-oder-pvctest:1.1.0.1',
        'arguments':["--job.names=firstoneJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3b5e7d717edd41cea11aaa20a67c4f5c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        tkdgns,
        gnstkd,
        tkdtkdgns,
        Complete
    ]) 

    # authCheck >> tkdgns >> gnstkd >> tkdtkdgns >> Complete
    workflow









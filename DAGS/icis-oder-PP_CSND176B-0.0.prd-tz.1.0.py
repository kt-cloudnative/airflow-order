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
                , WORKFLOW_NAME='PP_CSND176B',WORKFLOW_ID='f98eef5e7315423faf626fce6640f309', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND176B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f98eef5e7315423faf626fce6640f309')

    csnd176bJob_vol = []
    csnd176bJob_volMnt = []
    csnd176bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd176bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd176bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd176bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd176bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd176bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd4e20b9ea772401696b9e942f68f8d8e',
        'volumes': csnd176bJob_vol,
        'volume_mounts': csnd176bJob_volMnt,
        'env_from':csnd176bJob_env,
        'task_id':'csnd176bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csnd176bJob", "endTranDate=20240628", "pgmNm=csnd176b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f98eef5e7315423faf626fce6640f309')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd176bJob,
        Complete
    ]) 

    # authCheck >> csnd176bJob >> Complete
    workflow









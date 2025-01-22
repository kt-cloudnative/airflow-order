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
                , WORKFLOW_NAME='EI_CBOT071B',WORKFLOW_ID='a33dfec8797449a583d9de7af7e79b26', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT071B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a33dfec8797449a583d9de7af7e79b26')

    cbot071bJob_vol = []
    cbot071bJob_volMnt = []
    cbot071bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot071bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot071bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot071bJob_env.extend([getICISConfigMap('icis-oder-entprinet-batch-mng-configmap'), getICISSecret('icis-oder-entprinet-batch-mng-secret'), getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISSecret('icis-oder-entprinet-batch-secret')])
    cbot071bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot071bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ac0e10d55b2640b4a02877c9c80f9f7b',
        'volumes': cbot071bJob_vol,
        'volume_mounts': cbot071bJob_volMnt,
        'env_from':cbot071bJob_env,
        'task_id':'cbot071bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.33',
        'arguments':["--job.name=cbot071bJob", "requestDate=${YYYYMMDDHHMISS}", "procDate=20241129", "progName=cbot071b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a33dfec8797449a583d9de7af7e79b26')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot071bJob,
        Complete
    ]) 

    # authCheck >> cbot071bJob >> Complete
    workflow









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
                , WORKFLOW_NAME='IC_CNTRYINFOUSSREVIV',WORKFLOW_ID='3124a36b2aea49b9b5ab83b406a1e3f0', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IC_CNTRYINFOUSSREVIV-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 11, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3124a36b2aea49b9b5ab83b406a1e3f0')

    cntryInfoUssRevivJob_vol = []
    cntryInfoUssRevivJob_volMnt = []
    cntryInfoUssRevivJob_vol.append(getVolume('shared-volume','shared-volume'))
    cntryInfoUssRevivJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cntryInfoUssRevivJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cntryInfoUssRevivJob_env.extend([getICISConfigMap('icis-oder-infocomm-batch-mng-configmap'), getICISSecret('icis-oder-infocomm-batch-mng-secret'), getICISConfigMap('icis-oder-infocomm-batch-configmap'), getICISSecret('icis-oder-infocomm-batch-secret')])
    cntryInfoUssRevivJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cntryInfoUssRevivJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a1dfcade84044d92b90d2ff2e5211c2f',
        'volumes': cntryInfoUssRevivJob_vol,
        'volume_mounts': cntryInfoUssRevivJob_volMnt,
        'env_from':cntryInfoUssRevivJob_env,
        'task_id':'cntryInfoUssRevivJob',
        'image':'/icis/icis-oder-infocomm-batch:0.7.1.6',
        'arguments':["--job.name=cntryInfoUssRevivJob", "reqDate=${YYYYMMDDHHMISS}", "procDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3124a36b2aea49b9b5ab83b406a1e3f0')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cntryInfoUssRevivJob,
        Complete
    ]) 

    # authCheck >> cntryInfoUssRevivJob >> Complete
    workflow









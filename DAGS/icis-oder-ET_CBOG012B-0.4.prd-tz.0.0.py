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
                , WORKFLOW_NAME='ET_CBOG012B',WORKFLOW_ID='bbd60b18dfcd431184a3ba119b9381e5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CBOG012B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 2, 13, 38, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('bbd60b18dfcd431184a3ba119b9381e5')

    cbog012bJob_vol = []
    cbog012bJob_volMnt = []
    cbog012bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog012bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog012bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog012bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    cbog012bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog012bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f3334b93342e415ab47eb51511412f54',
        'volumes': cbog012bJob_vol,
        'volume_mounts': cbog012bJob_volMnt,
        'env_from':cbog012bJob_env,
        'task_id':'cbog012bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.6',
        'arguments':["--job.name=cbog012bJob", "requestDate=${YYYYMMDDHHMISSSSS}", "reqYm=${YYYYMM}", "regEmpNo=cbog012b", "regOfcCd=710571", "vCustNo=N"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('bbd60b18dfcd431184a3ba119b9381e5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog012bJob,
        Complete
    ]) 

    # authCheck >> cbog012bJob >> Complete
    workflow








